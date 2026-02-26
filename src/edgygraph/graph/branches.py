from __future__ import annotations
from typing import cast, Sequence, Hashable
from collections import defaultdict
from collections.abc import Hashable
import asyncio

from ..nodes import START, END, Node
from ..states import StateProtocol, SharedProtocol
from ..diff import Change
from .types import NodeTupel, Edge, ErrorEdge, SingleNext, Entry, ErrorEntry, SingleSource, SingleErrorSource, Config, ErrorConfig, is_node_tupel


class Branch[T: StateProtocol, S: SharedProtocol]:
    """
    A branch of the graph.


    Args:
        edges: The edges of the branch.
        join: The node to join the branch after execution. If None the branch will not be joined.

    """
    
    edges: Sequence[Edge[T, S] | ErrorEdge[T, S] | NodeTupel[T, S]]

    join: SingleNext[T, S]

    result: asyncio.Future[dict[tuple[Hashable, ...], Change]] | None = None

    edge_index: dict[SingleSource[T, S], list[Entry[T, S]]]
    error_edge_index: dict[SingleErrorSource[T, S], list[ErrorEntry[T, S]]]


    def __init__(self, edges: Sequence[Edge[T, S] | ErrorEdge[T, S] | NodeTupel[T, S]], join: SingleNext[T, S] = None) -> None:

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