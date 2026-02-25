from __future__ import annotations
from typing import cast, Any, Sequence, Hashable, TYPE_CHECKING
from collections import defaultdict
from collections.abc import Hashable
import asyncio

from ..nodes import START, END, Node
from ..states import StateProtocol, SharedProtocol
from ..diff import Diff, Change
from .types import NodeTupel, Edge, ErrorEdge, SingleNext, Entries, Entry, ErrorEntry, SingleSource, SingleErrorSource, Config, ErrorConfig, NextNode, is_node_tupel

if TYPE_CHECKING:
    from .graphs import Graph


class Branch[T: StateProtocol, S: SharedProtocol]:

    """
    A branch of the graph.
    """
    
    graph: Graph[T, S]
    edges: Sequence[Edge[T, S] | ErrorEdge[T, S] | NodeTupel[T, S]]

    join: SingleNext[T, S]

    result: asyncio.Future[dict[tuple[Hashable, ...], Change]] | None = None

    edge_index: dict[SingleSource[T, S], list[Entry[T, S]]]
    error_edge_index: dict[SingleErrorSource[T, S], list[ErrorEntry[T, S]]]


    def __init__(self, graph: Graph[T, S], edges: Sequence[Edge[T, S] | ErrorEdge[T, S] | NodeTupel[T, S]], join: SingleNext[T, S] = None) -> None:

        self.graph = graph
        self.edges = edges
        self.join = join

        self.edge_index = defaultdict(list)
        self.error_edge_index = defaultdict(list)

        self.index_edges()


    def index_edges(self) -> None:
        """
        Index the edges by single source.

        Append the edges to
        - `edge_index` if the edge is a normal edge
        - `error_edge_index` if the edge is an error edge
        """

        for i, edge in enumerate(self.edges):

            # Node Sequence
            if is_node_tupel(edge):
                edge = cast(NodeTupel[T, S], edge)
                for source, next in zip(edge, edge[1:]):
                    if isinstance(source, type): assert source is START, f"Unexpected type in node sequence: {source}"
                    if isinstance(next, type): assert next is END, f"Unexpected type in node sequence: {next}"
                    assert isinstance(source, (Node, type)), f"Unexpected source type in node sequence: {source}"
                    self.edge_index[source].append(Entry[T, S](next=next, config=Config(), index=i))
                continue

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

    
    async def __call__(self, state: T, shared: S) -> None:
        """
        Execute the branch based on the edges

        Args:
            state: State of the first generic type of the graph or a subtype
            shared: Shared of the second generic type of the graph or a subtype

        Returns:
            New State instance and the same Shared instance
        """

        self.result = asyncio.Future()

        initial_state = state.model_copy()

        try:
            
            next_nodes: list[NextNode[T, S]] = await self.get_next(state, shared, START)


            while next_nodes:

                # Hook
                for h in self.graph.hooks: await h.on_step_start(state, shared, next_nodes)

                # Run parallel
                result_states: list[T] = []

                state = await self.join_branches(state, next_nodes)

                try:

                    async with asyncio.TaskGroup() as tg:
                        for node in next_nodes:
                            
                            state_copy: T = state.model_copy(deep=True)
                            result_states.append(state_copy)

                            tg.create_task(self.node_wrapper(state_copy, shared, node))

                    # Merge
                    state = await self.graph.merge_states(state, result_states)


                except ExceptionGroup as eg:

                    print("ERROR")
                    print(eg)

                    # Hook
                    for h in self.graph.hooks: await h.on_step_end(state, shared, next_nodes)
                    
                    next_nodes = await self.get_next_from_error(state, shared, eg)

                    print(next_nodes)
                    
                else:
    
                    # Hook
                    for h in self.graph.hooks: await h.on_step_end(state, shared, next_nodes)

                    next_nodes = await self.get_next(state, shared, next_nodes)
        

        except Exception as e:
            
            # Hook
            for h in self.graph.hooks:
                e = await h.on_error(e, state, shared)
                if e is None: 
                    break
            
            if e:
                raise e
        
        self.result.set_result(Diff.recursive_diff(initial_state.model_dump(), state.model_dump()))

        

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
        

    async def join_branches(self, state: T, next_nodes: list[NextNode[T, S]]) -> T:
        """
        Join all branches that join on one of the next nodes.

        Args:
            state: The state of the graph.
            next_nodes: The next nodes to execute.

        Returns:
            The merged state of the graph after joining the branches.
        """

        state_dict = cast(dict[Hashable, Any], state.model_dump())

        for node in next_nodes:
            for branch in self.graph.join_registry[node.node]:

                if branch.result is None:
                    raise ValueError(f"Branch {branch} has no result")

                Diff.apply_changes(
                    state_dict,
                    await branch.result
                )

                self.graph.join_registry[node.node].remove(branch)

        return state.model_validate(state_dict)



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
                await self.resolve_entries_and_spawn_branches(state, shared, self.edge_index[START])
            )

        else: # Regular nodes
            for current_node in current_nodes:

                # Find the edge corresponding to the current node
                next_list.extend(
                    await self.resolve_entries_and_spawn_branches(state, shared, self.edge_index[current_node.node])
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
            
            current_instant_next_list = await self.resolve_entries_and_spawn_branches(state, shared, current_entries)
            next_list.extend(current_instant_next_list)


        return next_list
    


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
                    next_nodes.extend(
                        await self.resolve_entries_and_spawn_branches(state, shared, [entry])
                    )

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


    async def resolve_entries_and_spawn_branches(self, state: T, shared: S, entries: Sequence[Entries[T, S]]) -> list[NextNode[T, S]]:
        
        for branch in [branch for branch in entries if isinstance(branch, Branch)]:
            self.graph.spawn_branch(state, shared, branch)

        return [
            next_node
            for entry in entries
            for next_node in await entry(state, shared, self.graph) 
            if isinstance(next_node, NextNode)
        ]
