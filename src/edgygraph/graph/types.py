from __future__ import annotations
from typing import Callable, Awaitable, Any, TypeGuard, cast
from pydantic import BaseModel, ConfigDict, Field

from ..states import StateProtocol, SharedProtocol
from ..nodes import Node, START, END


# type NodeTupel[T: StateProtocol, S: SharedProtocol] = tuple[SingleSource[T, S], *tuple[Node[T, S], ...]] | tuple[SingleSource[T, S], *tuple[Node[T, S], ...], Next[T, S]]

type SingleSource[T: StateProtocol, S: SharedProtocol] = Node[T, S] | type[START]
type Source[T: StateProtocol, S: SharedProtocol] = SingleSource[T, S] | list[SingleSource[T, S]]

type SingleErrorSource[T: StateProtocol, S: SharedProtocol] = type[Exception] | tuple[Node[T, S], type[Exception]]
type ErrorSource[T: StateProtocol, S: SharedProtocol] = SingleErrorSource[T, S] | list[SingleErrorSource[T, S]]

type SingleNext[T: StateProtocol, S: SharedProtocol] = Node[T, S] | None
type ResolvedNext[T: StateProtocol, S: SharedProtocol] = SingleNext[T, S] | list[SingleNext[T, S]]
type Next[T: StateProtocol, S: SharedProtocol] = ResolvedNext[T, S] | Callable[[T, S], ResolvedNext[T, S]] | Callable[[T, S], Awaitable[ResolvedNext[T, S]]]


type Edge[T: StateProtocol, S: SharedProtocol] = tuple[Source[T, S], Next[T, S]] | tuple[Source[T, S], Next[T, S], Config]
type ErrorEdge[T: StateProtocol, S: SharedProtocol] = tuple[ErrorSource[T, S], Next[T, S]] | tuple[ErrorSource[T, S], Next[T, S], ErrorConfig]

type Join[T: StateProtocol, S: SharedProtocol] = Next[T, S] | type[END]
type BranchContainer[T: StateProtocol, S: SharedProtocol] = tuple[Source[T, S], Next[T, S], *tuple[Edge[T, S] | ErrorEdge[T, S] | Node[T, S] | SingleErrorSource[T, S] | Next[T, S], ...], Join[T, S]]
type SingleSourceBranchContainer[T: StateProtocol, S: SharedProtocol] = tuple[SingleSource[T, S], Next[T, S], *tuple[Edge[T, S] | ErrorEdge[T, S] | Node[T, S] | SingleErrorSource[T, S] | Next[T, S], ...], Join[T, S]]


class Types[T: StateProtocol, S: SharedProtocol]:
    """
    Typeguards for runtime typechecking.
    """

    @classmethod
    def is_next(cls, x: Any) -> TypeGuard[Next[T, S]]:
        return (
            cls.is_resolved_next(x) or
            callable(x)
        )

    @classmethod
    def is_resolved_next(cls, x: Any) -> TypeGuard[ResolvedNext[T, S]]:
        return (
            cls.is_single_next(x) or
            cls.is_single_next_list(x)
        )

    @classmethod
    def is_single_next_list(cls, x: Any) -> TypeGuard[list[SingleNext[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_next(n) for n in cast(list[Any], x))


    @classmethod
    def is_single_next(cls, x: Any) -> TypeGuard[SingleNext[T, S]]:
        return (
            x is None or
            isinstance(x, Node)
        )

    @classmethod
    def is_source(cls, x: Any) -> TypeGuard[Source[T, S]]:
        return (
            cls.is_single_source(x) or
            cls.is_single_source_list(x)
        )

    @classmethod
    def is_single_source(cls, x: Any) -> TypeGuard[SingleSource[T, S]]:
        return (
            x is START or
            isinstance(x, Node)
        )

    @classmethod
    def is_single_source_list(cls, x: Any) -> TypeGuard[list[SingleSource[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_source(n) for n in cast(list[Any], x))

    @classmethod
    def is_error_source(cls, x: Any) -> TypeGuard[ErrorSource[T, S]]:
        return (
            cls.is_single_error_source(x) or
            cls.is_single_error_source_list(x)
        )

    @classmethod
    def is_single_error_source(cls, x: Any) -> TypeGuard[SingleErrorSource[T, S]]:
        return (
            (isinstance(x, type) and issubclass(x, Exception)) or
            (isinstance(x, tuple) and len(cast(tuple[Any], x)) == 2 and isinstance(x[0], Node) and isinstance(x[1], type) and issubclass(x[1], Exception))
        )
    
    @classmethod
    def is_single_error_source_list(cls, x: Any) -> TypeGuard[list[SingleErrorSource[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_error_source(n) for n in cast(list[Any], x))
    
    @classmethod
    def is_any_source(cls, x: Any) -> TypeGuard[Source[T, S] | ErrorSource[T, S]]:
        return cls.is_source(x) or cls.is_error_source(x)
    
    @classmethod
    def is_any_single_source(cls, x: Any) -> TypeGuard[SingleSource[T, S] | SingleErrorSource[T, S]]:
        return cls.is_single_source(x) or cls.is_single_error_source(x)
    
    @classmethod
    def is_any_single_source_list(cls, x: Any) -> TypeGuard[list[SingleSource[T, S]] | list[SingleErrorSource[T, S]]]:
        return cls.is_single_source_list(x) or cls.is_single_error_source_list(x)
    
    @classmethod
    def is_join(cls, x: Any) -> TypeGuard[Join[T, S]]:
        return x is END or cls.is_next(x)

    @classmethod
    def is_edge(cls, x: Any) -> TypeGuard[Edge[T, S]]:
        return (
            isinstance(x, tuple) and len(cast(tuple[Any], x)) in (2, 3) and cls.is_source(x[0]) and cls.is_next(x[1]) and (len(cast(tuple[Any], x)) == 2 or isinstance(x[2], Config))
        )
    
    @classmethod
    def is_error_edge(cls, x: Any) -> TypeGuard[ErrorEdge[T, S]]:
        return (
            isinstance(x, tuple) and len(cast(tuple[Any], x)) in (2, 3) and cls.is_error_source(x[0]) and cls.is_next(x[1]) and (len(cast(tuple[Any], x)) == 2 or isinstance(x[2], ErrorConfig))
        )
    
    @classmethod
    def is_any_edge(cls, x: Any) -> TypeGuard[Edge[T, S] | ErrorEdge[T, S]]:
        return cls.is_edge(x) or cls.is_error_edge(x)


class Config(BaseModel):
    """
    Configuration for the edge.

    Attributes:
        instant: If the edge should be executed parallel to the source node. Instant edges are traversed recursively. Make sure to avoid infinite loops.
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
    Base class for the values of edge indexing dictionaries of a branch.

    Do not instantiate directly.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges of the branch.
    """

    next: Next[T, S]
    index: int
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any):
        if type(self) is BaseEntry:
            raise Exception("BaseEntry is not meant to be instantiated directly.") # Safeguard


class Entry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the edge indexing dictionary of a branch.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
        config: The configuration of the edge.
    """

    config: Config = Field(default_factory=Config)

class ErrorEntry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the error edge indexing dictionary of a branch.

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
