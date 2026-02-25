from __future__ import annotations
from typing import Callable, Awaitable, Any, TYPE_CHECKING
from pydantic import BaseModel, ConfigDict, Field
import inspect

from ..states import StateProtocol, SharedProtocol
from ..nodes import Node, START, END

if TYPE_CHECKING:
    from .graphs import Graph
    from .branches import Branch



type NodeTupel[T: StateProtocol, S: SharedProtocol] = tuple[Source[T, S], *tuple[Node[T, S], ...]] | tuple[Source[T, S], *tuple[Node[T, S], ...], Next[T, S]]

type SingleSource[T: StateProtocol, S: SharedProtocol] = Node[T, S] | type[START]
type Source[T: StateProtocol, S: SharedProtocol] = SingleSource[T, S] | list[SingleSource[T, S]]

type SingleErrorSource[T: StateProtocol, S: SharedProtocol] = type[Exception] | tuple[Node[T, S], type[Exception]]
type ErrorSource[T: StateProtocol, S: SharedProtocol] = SingleErrorSource[T, S] | tuple[list[Node[T, S]], type[Exception]]

type SingleNext[T: StateProtocol, S: SharedProtocol] = Node[T, S] | type[END] | None
type BranchNext[T: StateProtocol, S: SharedProtocol] = tuple[list[Edge[T, S]], SingleNext[T, S]]
type ResolvedNext[T: StateProtocol, S: SharedProtocol] = SingleNext[T, S] | list[SingleNext[T, S]] | BranchNext[T, S]
type Next[T: StateProtocol, S: SharedProtocol] = ResolvedNext[T, S] | Callable[[T, S], ResolvedNext[T, S]] | Callable[[T, S], Awaitable[ResolvedNext[T, S]]]


type Edge[T: StateProtocol, S: SharedProtocol] = tuple[Source[T, S], Next[T, S]] | tuple[Source[T, S], Next[T, S], Config]
type ErrorEdge[T: StateProtocol, S: SharedProtocol] = tuple[ErrorSource[T, S], Next[T, S]] | tuple[ErrorSource[T, S], Next[T, S], ErrorConfig]


def is_node_tupel(edge: tuple[Any, ...]) -> bool:

    if len(edge) < 2:
        return False
    
    if is_source(edge[0]) and is_only_node_tuple(edge[1:-1]) and is_next(edge[-1]):
        return True
    
    if is_only_node_tuple(edge[:-1]) and is_next(edge[-1]):
        return True
    
    return False

def is_only_node_tuple(edge: tuple[Any, ...]) -> bool:
    return all(isinstance(n, Node) for n in edge)


def is_next(x: Any) -> bool:
    return (
        is_single_next(x) or
        (isinstance(x, list) and all(is_single_next(n) for n in x)) or # type: ignore
        callable(x) # type: ignore
    )

def is_single_next(x: Any) -> bool:
    return (
        x is None or
        x is END or
        isinstance(x, Node)
    )

def is_source(x: Any) -> bool:
    return (
        is_single_source(x) or
        (isinstance(x, list) and all(is_single_source(n) for n in x)) # type: ignore
    )

def is_single_source(x: Any) -> bool:
    return (
        x is START or   
        isinstance(x, Node)
    )


class Config(BaseModel):
    """
    Configuration for the edge.

    Attributes:
        instant: If the edge should be executed parallel to the source node. Instant edges are traversed recursively. Be sure to avoid infinite loops.
    """

    instant: bool = False

class ErrorConfig(BaseModel):
    """
    Configuration for the error edge.

    Attributes:
        propagate: If the error should be propagated to the next error edge. If False, the error is caught and the graph continues.
    """

    propagate: bool = False


class BaseEntry[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    Base class for the values of edge indexing dictionaries of the graph.

    Do not instantiate directly.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
    """

    next: Next[T, S]
    index: int
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any):
        if type(self) is BaseEntry:
            raise Exception("BaseEntry is not meant to be instantiated directly.") # Safeguard

    async def __call__(self, state: T, shared: S, graph: Graph[T, S]) -> list[NextNode[T, S] | Branch[T, S]]:
        """
        Resolve the next to nodes.

        Make sure to ONLY call this method ONCE per execution of the corresponding edge.
    
        Args:
            state: The current state.
            shared: The shared state.
        
        Returns:
            The resolved nodes.
        """

        from .branches import Branch

        next_nodes: list[NextNode[T, S] | Branch[T, S]] = []
        next = self.next

        match next:

            case None:
                pass # END

            case type():
                assert next is END, "Only END is allowed as a type here"
            
            case Node():
                next_nodes.append(NextNode[T, S](node=next, reached_by=self)) # type: ignore

            case list():
                for n in next:
                    if isinstance(n, Node):
                        next_nodes.append(NextNode[T, S](node=n, reached_by=self)) # type: ignore

            case (edges, join):
                next_nodes.append(Branch[T, S](graph=graph, edges=edges, join=join))


            case _: # callable
                next = next
                res = next(state, shared)
                if inspect.isawaitable(res):
                    res = await res # for awaitables
                
                if isinstance(res, Node):
                    next_nodes.append(NextNode[T, S](node=res, reached_by=self)) # type: ignore

        
        return next_nodes


class Entry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the edge indexing dictionary of the graph.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
        config: The configuration of the edge.
    """

    config: Config = Field(default_factory=Config)

class ErrorEntry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the error edge indexing dictionary of the graph.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
        config: The configuration of the edge.
    """

    config: ErrorConfig = Field(default_factory=ErrorConfig)


type Entries[T: StateProtocol, S: SharedProtocol] = Entry[T, S] | ErrorEntry[T, S]


class NextNode[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    A node that is the target of an edge.

    Attributes:
        node: The node.
        reached_by: The edge that targeted this node.
    """

    node: Node[T, S]
    reached_by: Entries[T, S]
    model_config = ConfigDict(arbitrary_types_allowed=True)
