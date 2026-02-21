
from typing import Tuple, cast, Any, Sequence
from collections import defaultdict
from collections.abc import Hashable
import asyncio
from pydantic import BaseModel, ConfigDict, Field

from .nodes import START, Node
from .states import StateProtocol as State, SharedProtocol as Shared
from .graph_hooks import GraphHook
from .diff import Change, ChangeConflictException, Diff
from .types import  \
    Edge, ErrorEdge, \
    SingleSource, SingleErrorSource, \
    Config, ErrorConfig, \
    Entry, ErrorEntry, Entries, \
    NextNode

    
    




class Graph[T: State = State, S: Shared = Shared](BaseModel):
    """
    Create and execute a graph defined by a list of edges.

    Set the required State and Shared classes via the Generic Typing Parameters.
    Because of variance its possible to use nodes, that use more general State and Shared classes (ancestors) as the Generic Typing Parameters.
    For the more flexible duck typing approch, that scales easier, use StateProtocol and SharedProtocol as Generic Typing Parameters.

    The edges are defined as a list of tuples, where the first element is the source node and the second element reveals the next node.

    Attributes:
        edges: A list of edges of compatible nodes that build the graph
        instant_edges: A list of edges of compatible nodes that run parallel to there source node
        error_edges: A list of edges of compatible nodes that run if the source node raises an exception
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    edges: Sequence[Edge[T, S] | ErrorEdge[T, S]] = Field(default_factory=list[Edge[T, S] | ErrorEdge[T, S]])
    hooks: Sequence[GraphHook[T, S]] = Field(default_factory=list[GraphHook[T, S]], exclude=True)

    edge_index: dict[SingleSource[T, S], list[Entry[T, S]]] = Field(default_factory=lambda: defaultdict(list), init=False)
    error_edge_index: dict[SingleErrorSource[T, S], list[ErrorEntry[T, S]]] = Field(default_factory=lambda: defaultdict(list), init=False)


    def model_post_init(self, _) -> None:
        """
        Index the edges after the model is initialized.
        """

        self.index_edges()

        

    def index_edges(self) -> None:
        """
        Index the edges by single source.

        Append the edges to
        - `edge_index` if the edge is a normal edge
        - `error_edge_index` if the edge is an error edge
        """

        for i, edge in enumerate(self.edges):

            match edge:
                case (source, next, config): pass
                case (source, next): config = Config() if source is START or isinstance(source, (Node, list)) else ErrorConfig()
                case _: raise ValueError(f"Invalid edge format: {edge}")
                
            if (isinstance(source, type) and issubclass(source, Exception)): # Error edge
                assert isinstance(config, ErrorConfig), f"Unexpected properties type for error edge: {config}"
                self.error_edge_index[source].append(ErrorEntry[T, S](next=next, config=config, index=i))

            elif isinstance(source, tuple): # Error edge with nodes
                assert isinstance(config, ErrorConfig), f"Unexpected properties type for error edge: {config}"
                nodes = source[0] if isinstance(source[0], list) else [source[0]]
                et = source[1]

                for node in nodes:
                    self.error_edge_index[(node, et)].append(ErrorEntry[T, S](next=next, config=config, index=i))

            elif isinstance(source, list): # Multiple sources
                assert isinstance(config, Config), f"Unexpected properties type for node edge: {config}"

                for s in source:
                    self.edge_index[s].append(Entry[T, S](next=next, config=config, index=i))

            elif isinstance(source, Node) or source is START: # Single source
                assert isinstance(config, Config), f"Unexpected properties type for node edge: {config}"
                self.edge_index[source].append(Entry[T, S](next=next, config=config, index=i))

            else:
                raise ValueError(f"Invalid edge source: {edge[0]}")



    async def __call__(self, state: T, shared: S) -> Tuple[T, S]:
        """
        Execute the graph based on the edges

        Args:
            state: State of the first generic type of the graph or a subtype
            shared: Shared of the second generic type of the graph or a subtype

        Returns:
            New State instance and the same Shared instance
        """

        try:

            # Hook
            for h in self.hooks: await h.on_graph_start(state, shared)

            
            next_nodes: list[NextNode[T, S]] = await self.get_next(state, shared, START)


            while True:

                # Hook
                for h in self.hooks: await h.on_step_start(state, shared, next_nodes)

                if not next_nodes:
                    break # END

                # Run parallel
                result_states: list[T] = []

                try:

                    async with asyncio.TaskGroup() as tg:
                        for node in next_nodes:
                            
                            state_copy: T = state.model_copy(deep=True)
                            result_states.append(state_copy)

                            tg.create_task(self.node_wrapper(state_copy, shared, node))

                    state = await self.merge_states(state, result_states)


                except ExceptionGroup as eg:

                    print("ERROR")
                    print(eg)

                    # Hook
                    for h in self.hooks: await h.on_step_end(state, shared, next_nodes)
                    
                    next_nodes = await self.get_next_from_error(state, shared, eg)

                    print(next_nodes)
                    
                else:
    
                    # Hook
                    for h in self.hooks: await h.on_step_end(state, shared, next_nodes)

                    next_nodes = await self.get_next(state, shared, next_nodes)


            # Hook
            for h in self.hooks: await h.on_graph_end(state, shared)


            return state, shared
        

        except Exception as e:
            
            # Hook
            for h in self.hooks:
                e = await h.on_error(e, state, shared)
                if e is None: 
                    break
            
            if e:
                raise e
        
            return state, shared


    async def node_wrapper(self, state: T, shared: S, node: NextNode[T, S]):
        """
        Wrapper for the nodes to catch exceptions and add the node to the exception with the key: `source_node`.
        
        This is used to determine the node that caused the exception.
        This is used in the `get_next_nodes_from_error` method to determine the next nodes to execute.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            node: The node to execute.
        """

        try:
            await node.node(state, shared)

        except Exception as e:
            e.source_node = node # type: ignore
            raise e
        

    
    async def get_next_from_error(self, state: T, shared: S, eg: ExceptionGroup) -> list[NextNode[T, S]]:
        """
        Get the next nodes to execute from the error.

        If exceptions in the group dont have the key `source_node` they will be reraised as an Exception group.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            eg: The exception group with all errors to get the next nodes from.

        Returns:
            The next nodes to execute.
        """

        next_nodes: list[NextNode[T, S]] = []
        unhandled: list[Exception] = []

        for e in eg.exceptions:

            print(e)

            source_node: NextNode[T, S] | None = getattr(e, "source_node", None)

            if not isinstance(source_node, NextNode):
                unhandled.append(e)
                continue

            entries: list[ErrorEntry[T, S]] = []

            for key in self.error_edge_index.keys():
                
                if self.match_error(e, key, source_node):
                    entries.extend(self.error_edge_index[key])

            entries.sort(key=lambda x: x.index)
            for entry in entries:
                if entry.index > source_node.reached_by.index: # If the error entry is after the node that raised the error
                    next_nodes.extend(await entry(state, shared))
                    print(entry)
                    if not entry.config.propagate:
                        break
            else: # Not consumed
                unhandled.append(e)

        
        if unhandled:
            raise ExceptionGroup("Unhandled node exceptions", unhandled)

        return next_nodes


    def match_error(self, e: Exception, source: SingleErrorSource[T, S], source_node: NextNode[T, S]) -> bool:

        match source:
            case type(): # Exception type
                return issubclass(type(e), source)
            case (node, error_type): # (Node, Exception type)
                return node == source_node.node and isinstance(e, error_type)


    async def get_next(self, state: T, shared: S, current_nodes: Sequence[NextNode[T, S]] | type[START]) -> list[NextNode[T, S]]:
        """
        Get the next nodes to run based on the current nodes and the graph's edges.

        Callable edges are called with the state and shared state.

        Args:
            state: The current state
            shared: The shared state
            current_nodes: The current nodes

        Returns:
           The list of the next nodes including their edges that they were reached by.
        """


        next_list: list[NextNode[T, S]] = []

        if isinstance(current_nodes, type): # START
            next_list.extend(
                await self.resolve_entries(state, shared, self.edge_index[START])
            )

        else: # Regular nodes
            for current_node in current_nodes:

                # Find the edge corresponding to the current node
                next_list.extend(
                    await self.resolve_entries(state, shared, self.edge_index[current_node.node])
                )


        # Instant nodes
        current_instant_next_list: list[NextNode[T, S]] = []

        while True:

            current_entries = [
                entry
                for next in current_instant_next_list 
                for entry in self.edge_index[next.node]
                if entry.config.instant
            ]
            
            if not current_entries:
                break
            
            current_instant_next_list = await self.resolve_entries(state, shared, current_entries)
            next_list.extend(current_instant_next_list)


        return next_list


    async def resolve_entries(self, state: T, shared: S, entries: Sequence[Entries[T, S]]) -> list[NextNode[T, S]]:

        return [
            next_node
            for entry in entries
            for next_node in await entry(state, shared)
        ]


    async def merge_states(self, current_state: T, result_states: list[T]) -> T:
        """
        Merges the result states into the current state.
        First the changes are calculated for each result state.
        Then the changes are checked for conflicts.
        If there are conflicts, a ChangeConflictException is raised.
        The changes are applied in the order of the result states list.

        Args:
            current_state: The current state
            result_states: The result states

        Returns:
            The new merged State instance.

        Raises:
            ChangeConflictException: If there are conflicts in the changes.
        """
            
        result_dicts = [state.model_dump() for state in result_states]
        current_dict = cast(dict[Hashable, Any], current_state.model_dump())
        state_class = type(current_state)

        changes_list: list[dict[tuple[Hashable, ...], Change]] = []

        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
        

        # Hook
        for h in self.hooks: await h.on_merge_start(current_state, result_states, changes_list)


        conflicts = Diff.find_conflicts(changes_list)

        if conflicts:

            # Hook
            for h in self.hooks: await h.on_merge_conflict(current_state, result_states, changes_list, conflicts)

            raise ChangeConflictException(f"Conflicts detected after parallel execution: {conflicts}")


        for changes in changes_list:
            Diff.apply_changes(current_dict, changes)

        state: T = state_class.model_validate(current_dict)


        # Hook
        for h in self.hooks: await h.on_merge_end(current_state, result_states, changes_list, state)

        return state
