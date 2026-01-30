from .nodes import Node, START, END
from .states import State, Shared
from .rich import RichReprMixin
from .logging import get_logger

from typing import Type, Callable, Coroutine, Tuple, Any, Awaitable
from collections import defaultdict
import asyncio
from pydantic import BaseModel, ConfigDict, Field
from enum import StrEnum, auto
from collections import Counter
import inspect

logger = get_logger(__name__)


type SourceType[T: State, S: Shared] = Node[T, S] | Type[START] | list[Node[T, S] | Type[START]]
type NextType[T: State, S: Shared] = Node[T, S] | Type[END] | Callable[[T, S], Node[T, S] | Type[END] | Awaitable[Node[T, S] | Type[END]]]
type Edge[T: State, S: Shared] = tuple[SourceType[T, S], NextType[T, S]]

class Graph[T: State = State, S: Shared = Shared](BaseModel):
    """
    Create and execute a graph defined by a list of edges.

    Set the required State and Shared classes via the Generic Typing Parameters.
    Because of variance its possible to use nodes, that use more general State and Shared classes (ancestors) as the Generic Typing Parameters. 

    The edges are defined as a list of tuples, where the first element is the source node and the second element reveals the next node.

    Arguments:
        T: The state class
        S: The shared state class
        edges: A list of edges of compatible nodes that build the graph
        instant_edge: A list of edges of compatible nodes that run parallel to there source node
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    edges: list[Edge[T, S]] = Field(default_factory=list[Edge[T, S]])
    instant_edges: list[Edge[T, S]] = Field(default_factory=list[Edge[T, S]])

    _edge_index: dict[Node[T, S] | Type[START], list[NextType[T, S]]] = defaultdict(list[NextType[T, S]])
    _instant_edge_index: dict[Node[T, S] | Type[START], list[NextType[T, S]]] = defaultdict(list[NextType[T, S]])


    def model_post_init(self, _) -> None:
        """
        Index the edges by source node
        """
        self._edge_index = self.index_edges(self.edges)
        self._instant_edge_index = self.index_edges(self.instant_edges)
        

    def index_edges(self, edges: list[Edge[T, S]]) -> dict[Node[T, S] | Type[START], list[NextType[T, S]]]:
        """
        Index the edges by source node

        Arguments:
           edges: The edges to index

        Returns:
            A mapping from source node (or START) to the next objects of the edge
            
        """
        
        edges_index: dict[Node[T, S] | Type[START], list[NextType[T, S]]] = defaultdict(list[NextType[T, S]])

        for edge in edges:
            sources = edge[0]
            if isinstance(sources, list):
                for source in sources:
                    edges_index[source].append(edge[1])
            else:
                edges_index[sources].append(edge[1])

        return edges_index




    async def __call__(self, state: T, shared: S) -> Tuple[T, S]:
        """
        Execute the graph based on the edges

        Arguments:
            state: State of the first generic type of the graph or a subtype
            shared: Shared of the second generic type of the graph or a subtype

        Returns:
            New State instance and the same Shared instance
        """
        
        current_nodes: list[Node[T, S]] | list[Node[T, S] | Type[START]] = [START]

        while True:

            next_nodes: list[Node[T, S]] = await self.get_next_nodes(state, shared, current_nodes, self._edge_index)

            if not next_nodes:
                break # END


            current_instant_nodes: list[Node[T, S]] = next_nodes.copy()
            while True:

                current_instant_nodes = await self.get_next_nodes(state, shared, current_instant_nodes, self._instant_edge_index)
                
                logger.debug("CURRENT INSTANT NODES: %s", current_instant_nodes)

                if not current_instant_nodes:
                    break
                
                next_nodes.extend(current_instant_nodes)

            logger.debug("NEXT NODES: %s", next_nodes)

            parallel_tasks: list[Callable[[T, S], Coroutine[None, None, None]]] = []


            # Extract the run function of the nodes
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

    async def get_next_nodes(self, state: T, shared: S, current_nodes: list[Node[T, S]] | list[Node[T, S] | Type[START]], edge_index: dict[Node[T, S] | Type[START], list[NextType[T, S]]]) -> list[Node[T, S]]:
        """
        Arguments:
            state: The current State
            shared: The Shared
            current_nodes: The current nodes

        Returns:
           The list of the next nodes to run based on the current nodes and edges
           If an edge is a callable, it will be called with the current State and Shared
        """


        next_types: list[NextType[T, S]] = []

        for current_node in current_nodes:

            # Find the edge corresponding to the current node
            next_types.extend(edge_index[current_node])


        next_nodes: list[Node[T, S]] = []
        for next in next_types:

            next = next

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
        """
        Merges the result States into the current State.
        First the changes are calculated for each result State.
        Then the changes are checked for conflicts.
        If there are conflicts, an exception is raised.
        The changes are applied in the order of the result States list.

        Arguments:
            current_state: The current State
            result_states: The result States

        Returns:
            The merged State, the same instance as the current State but with the changes applied
        """
            
        result_dicts = [state.model_dump() for state in result_states]
        current_dict = current_state.model_dump()
        state_class = type(current_state)

        changes_list: list[dict[str, Change]] = []

        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
        
        logger.debug(f"CHANGES: %s", changes_list)
        
        conflicts = Diff.find_conflicts(changes_list)

        if conflicts:
            raise Exception(f"Conflicts detected after parallel execution: {conflicts}")

        for changes in changes_list:
            Diff.apply_changes(current_dict, changes)

        state: T = state_class.model_validate(current_dict)

        logger.debug("NEW STATE: %s", state)

        return state
    
            

class ChangeTypes(StrEnum):
    """
    Enum for the types of changes that can be made to a State.
    """

    ADDED = auto()
    REMOVED = auto()
    UPDATED = auto()

class Change(RichReprMixin, BaseModel):
    """
    Represents a change made to a State.
    """

    type: ChangeTypes
    old: Any
    new: Any



class Diff:
    """
    Utility class for computing differences between states.
    """


    @classmethod
    def find_conflicts(cls, changes: list[dict[str, Change]]) -> dict[str, list[Change]]:
        """
        Finds conflicts in a list of changes.

        Arguments:
           changes: A list of dictionaries representing changes to a state.
        """

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
        """
        Recursively computes the differences between two dictionaries.


        Arguments:
            old: Part of the old dictionary.
            new: Part of the new dictionary.
            path: The current path of the parts in the full dictionary, seperated with dots.

        Returns:
            A mapping of the path to the changes directly on that level.
        """
        
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
    def apply_changes(cls, target: dict[str, Any], changes: dict[str, Change]) -> None:
        """
        Applies a set of changes to the target dictionary.


        Arguments:
            target: The dictionary to apply the changes to.
            changes: A mapping of paths, separated by dots, to changes. The changes are applied in the dictionary on that level.
        """

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