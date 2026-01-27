from .nodes import Node
from .states import State, Shared
from typing import Type, Callable, Coroutine, Tuple, Any, Awaitable
from collections import defaultdict
import asyncio
from pydantic import BaseModel, ConfigDict, Field
from enum import StrEnum, auto
from collections import Counter
from rich import print as rprint
import inspect

class START:
    pass

class END:
    pass


type SourceType[T: State, S: Shared] = Node[T, S] | Type[START] | list[Node[T, S] | Type[START]]
type NextType[T: State, S: Shared] = Node[T, S] | Type[END] | Callable[[T, S], Node[T, S] | Type[END] | Awaitable[Node[T, S] | Type[END]]]
type Edge[T: State, S: Shared] = tuple[SourceType[T, S], NextType[T, S]]

class Graph[T: State = State, S: Shared = Shared](BaseModel):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    edges: list[Edge[T, S]] = Field(default_factory=list[Edge[T, S]])
    instant_edges: list[Edge[T, S]] = Field(default_factory=list[Edge[T, S]])

    _edge_index: dict[Node[T, S] | Type[START], list[Edge[T, S]]] = defaultdict(list[Edge[T, S]])
    _instant_edge_index: dict[Node[T, S] | Type[START], list[Edge[T, S]]] = defaultdict(list[Edge[T, S]])


    def model_post_init(self, context: Any) -> None:
        self._edge_index = self.index_edges(self.edges)
        self._instant_edge_index = self.index_edges(self.instant_edges)
        

    def index_edges(self, edges: list[Edge[T, S]]) -> dict[Node[T, S] | Type[START], list[Edge[T, S]]]:
        
        edges_index: dict[Node[T, S] | Type[START], list[Edge[T, S]]] = defaultdict(list[Edge[T, S]])

        for edge in edges:
            sources = edge[0]
            if isinstance(sources, list):
                for source in sources:
                    edges_index[source].append(edge)
            else:
                edges_index[sources].append(edge)

        return edges_index




    async def __call__(self, state: T, shared: S) -> Tuple[T, S]:
        
        current_nodes: list[Node[T, S]] | list[Node[T, S] | Type[START]] = [START]

        while True:

            next_nodes: list[Node[T, S]] = await self.get_next_nodes(state, shared, current_nodes, self._edge_index)

            if not next_nodes:
                break # END


            current_instant_nodes: list[Node[T, S]] = next_nodes.copy()
            while True:

                current_instant_nodes = await self.get_next_nodes(state, shared, current_instant_nodes, self._instant_edge_index)
                
                rprint("INSTANT NODES")
                rprint(current_instant_nodes)

                if not current_instant_nodes:
                    break
                
                next_nodes.extend(current_instant_nodes)

            rprint("NEXT NODES")
            rprint(next_nodes)
            
            parallel_tasks: list[Callable[[T, S], Coroutine[None, None, None]]] = []

            # Determine the next node using the edge's next function
            for next_node in next_nodes:
                
                parallel_tasks.append(next_node.run)

            # Run parallel

            result_states: list[T] = []

            async with asyncio.TaskGroup() as tg:
                for task in parallel_tasks:
                    
                    state_copy: T = state.model_copy(deep=True)
                    result_states.append(state_copy)

                    tg.create_task(task(state_copy, shared))

            state = self.merge_states(state, result_states)

            current_nodes = next_nodes


        return state, shared

    async def get_next_nodes(self, state: T, shared: S, current_nodes: list[Node[T, S]] | list[Node[T, S] | Type[START]], edge_index: dict[Node[T, S] | Type[START], list[Edge[T, S]]]) -> list[Node[T, S]]:
        edges: list[Edge[T, S]] = []

        for current_node in current_nodes:

            # Find the edge corresponding to the current node
            edges.extend(edge_index[current_node])


        next_nodes: list[Node[T, S]] = []
        for edge in edges:

            next = edge[1]

            if next is END:
                continue

            if isinstance(next, Callable):
                res = next(state, shared) #type:ignore (its not an END!)
                if inspect.isawaitable(res):
                    res = await res # for awaitables
                
                if isinstance(res, Node):
                    next_nodes.append(res)
            
            else:
                next_nodes.append(next)
        
        return next_nodes


    def merge_states(self, current_state: T, result_states: list[T]) -> T:
            
        result_dicts = [state.model_dump() for state in result_states]
        current_dict = current_state.model_dump()
        state_class = type(current_state)

        changes_list: list[dict[str, Any]] = []

        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
        
        rprint("CHANGES")
        rprint(changes_list)
        
        conflicts = Diff.find_conflicts(changes_list)

        if conflicts:
            raise Exception(f"Conflicts detected after parallel execution: {conflicts}")

        for changes in changes_list:
            Diff.apply_patch(current_dict, changes)

        state: T = state_class.model_validate(current_dict)

        rprint("NEW STATE")
        rprint(state)

        return state
    
            

class ChangeTypes(StrEnum):
    ADDED = auto()
    REMOVED = auto()
    UPDATED = auto()

class Change(BaseModel):
    type: ChangeTypes
    old: Any
    new: Any


class Diff:

    @classmethod
    def find_conflicts(cls, changes: list[dict[str, Change]]) -> dict[str, list[Change]]:

        if len(changes) <= 1:
            return {}
        
        counts = Counter(key for d in changes for key in d)

        duplicate_keys = [k for k, count in counts.items() if count > 1]

        conflicts: dict[str, list[Change]] = {}        
        for key in duplicate_keys:
            conflicts[key] = [d[key] for d in changes if key in d]

        return conflicts


    @classmethod
    def recursive_diff(cls, old: Any, new: Any, path: str = "") -> dict[str, Change]:
        
        changes: dict[str, Change] = {}

        if isinstance(old, dict) and isinstance(new, dict):
            all_keys: set[str] = set(old.keys()) | set(new.keys()) #type: ignore

            for key in all_keys:
                current_path: str = f"{path}.{key}" if path else key

                if key in old and not key in new:
                    changes[current_path] = Change(type=ChangeTypes.REMOVED, old=old[key], new=None)
                elif key in new and not key in old:
                    changes[current_path] = Change(type=ChangeTypes.ADDED, old=None, new=new[key])
                else:
                    sub_changes = cls.recursive_diff(old[key], new[key], current_path)
                    changes.update(sub_changes)

        elif old != new:
            changes[path] = Change(type=ChangeTypes.UPDATED, old=old, new=new)

        return changes
    
    @classmethod
    def apply_patch(cls, target: dict[str, Any], changes: dict[str, Change]):

        for path, change in changes.items():
            parts = path.split(".")
            cursor = target
            
            # Navigate down the dictionary
            for part in parts[:-1]:
                if part not in cursor:
                    cursor[part] = {} # If the path was created because of ADDED
                cursor = cursor[part]
            
            last_key = parts[-1]

            if change.type == ChangeTypes.REMOVED:
                if last_key in cursor:
                    del cursor[last_key]
            else:
                # UPDATED or ADDED
                cursor[last_key] = change.new