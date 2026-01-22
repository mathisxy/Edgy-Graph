from .edges import Edge, START
from .nodes import Node
from .states import State, Shared
from typing import Type, TypeVar, Generic, Callable, Sequence, Coroutine, Tuple, Any
from collections import defaultdict
# from deepdiff import DeepDiff
import asyncio
from pydantic import BaseModel
from enum import StrEnum, auto
from collections import Counter
import inspect

T = TypeVar('T', bound=State)
S = TypeVar('S', bound=Shared)

class Graph(Generic[T, S]):

    edges: list[Edge[T, S]]

    def __init__(self, edges: list[Edge[T, S]]):
        self.edges = edges
        self._index: dict[Node[T, S] | Type[START], list[Edge[T, S]]] = defaultdict(list[Edge[T, S]])

        for edge in self.edges:
            if isinstance(edge.source, list):
                for source in edge.source:
                    self._index[source].append(edge)
            else:
                self._index[edge.source].append(edge)


    async def __call__(self, initial_state: T, initial_shared: S) -> Tuple[T, S]:
        state: T = initial_state
        shared: S = initial_shared
        current_nodes: Sequence[Node[T, S] | Type[START]] = [START]

        while True:

            edges: list[Edge[T, S]] = []

            for current_node in current_nodes:

                # Find the edge corresponding to the current node
                edges.extend(self._index[current_node])


            # next_nodes: list[Node[T, S]] = [
            #     n for edge in edges 
            #     if isinstance((n := edge.next(state, shared)), Node)
            # ] # Only one execution of next filtering "END"s

            next_nodes: list[Node[T, S]] = []
            for edge in edges:
                res = edge.next(state, shared)
                if inspect.isawaitable(res):
                    res = await res  # Hier wird die async-Funktion erst ausgeführt
                
                next_nodes.append(res)


            if not next_nodes:
                break # END

            
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

        


    def merge_states(self, current_state: T, result_states: list[T]) -> T:
            
        result_dicts = [state.model_dump() for state in result_states]
        current_dict = current_state.model_dump()
        state_class = type(current_state)


        # diffs = [DeepDiff(current_dict, result_dict) for result_dict in result_dicts]

        # print("DIFFS")
        # print(diffs)

        changes_list: list[dict[str, Any]] = []

        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
                    
        print(changes_list)
        
        conflicts = Diff.find_conflicts(changes_list)

        if conflicts:
            raise Exception(f"Conflicts detected after parallel execution: {conflicts}")

        for changes in changes_list:
            Diff.apply_patch(current_dict, changes)

        state: T = state_class.model_validate(current_dict)

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