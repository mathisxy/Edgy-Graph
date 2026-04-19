from __future__ import annotations
from typing import Callable, Awaitable, Any, TypeGuard, cast
from pydantic import BaseModel, ConfigDict, Field

from ..states import StateProtocol, SharedProtocol
from ..nodes import Node, START, END, NodeConfig


type Flexible[V] = V | list[V]

type NodeWithConfig[T: StateProtocol, S: SharedProtocol] = tuple[Node[T, S], NodeConfig]

type SingleSource[T: StateProtocol, S: SharedProtocol] = Node[T, S] | type[START]
type SingleSourceWithConfig[T: StateProtocol, S: SharedProtocol] = NodeWithConfig[T, S] | SingleSource[T, S]
type Source[T: StateProtocol, S: SharedProtocol] = Flexible[SingleSource[T, S]]
type SourceWithConfig[T: StateProtocol, S: SharedProtocol] = Flexible[SingleSourceWithConfig[T, S]]

type SingleErrorSource[T: StateProtocol, S: SharedProtocol] = type[Exception] | tuple[Node[T, S], type[Exception]]
type ErrorSource[T: StateProtocol, S: SharedProtocol] = Flexible[SingleErrorSource[T, S]]

type SingleNext[T: StateProtocol, S: SharedProtocol] = Node[T, S] | None
type SingleNextWithConfig[T: StateProtocol, S: SharedProtocol] = NodeWithConfig[T, S] | SingleNext[T, S]
type ResolvedNext[T: StateProtocol, S: SharedProtocol] = Flexible[SingleNext[T, S]]
type ResolvedNextWithConfig[T: StateProtocol, S: SharedProtocol] = Flexible[SingleNextWithConfig[T, S]]
type Next[T: StateProtocol, S: SharedProtocol] = ResolvedNext[T, S] | Callable[[T, S], ResolvedNext[T, S]] | Callable[[T, S], Awaitable[ResolvedNext[T, S]]]
type NextWithConfig[T: StateProtocol, S: SharedProtocol] = ResolvedNextWithConfig[T, S] | Callable[[T, S], ResolvedNext[T, S]] | Callable[[T, S], Awaitable[ResolvedNext[T, S]]] # Dynamic parameters cannot have a NodeConfig, because it is used for indexing.


type BranchJoin[T: StateProtocol, S: SharedProtocol] = SingleNext[T, S] | type[END]

type BranchContainer[T: StateProtocol, S: SharedProtocol] = tuple[Source[T, S], *tuple[SourceWithConfig[T, S] | ErrorSource[T, S] | NextWithConfig[T, S], ...], BranchJoin[T, S]]
# type SingleSourceBranchContainer[T: StateProtocol, S: SharedProtocol] = tuple[SingleBranchSource[T, S], NextWithConfig[T, S], *tuple[SourceWithConfig[T, S] | ErrorSource[T, S] | NextWithConfig[T, S], ...], BranchJoin[T, S]]


class Types[T: StateProtocol, S: SharedProtocol]:
    """
    Typeguards for runtime typechecking.
    """

    @classmethod
    def is_node_with_config(cls, x: Any) -> TypeGuard[NodeWithConfig[T, S]]:
        return (
            isinstance(x, tuple) and
            len(cast(tuple[Any], x)) == 2 and
            isinstance(x[0], Node) and
            isinstance(x[1], NodeConfig)
        )


    @classmethod
    def is_single_next(cls, x: Any) -> TypeGuard[SingleNext[T, S]]:
        return (
            x is None or
            isinstance(x, Node)
        )
    
    @classmethod
    def is_single_next_with_config(cls, x: Any) -> TypeGuard[SingleNextWithConfig[T, S]]:
        return (
            cls.is_single_next(x) or
            cls.is_node_with_config(x)
        )
    
    @classmethod
    def is_single_next_list(cls, x: Any) -> TypeGuard[list[SingleNext[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_next(n) for n in cast(list[Any], x))
    
    @classmethod
    def is_single_next_with_config_list(cls, x: Any) -> TypeGuard[list[SingleNextWithConfig[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_next_with_config(n) for n in cast(list[Any], x))
    
    @classmethod
    def is_resolved_next(cls, x: Any) -> TypeGuard[ResolvedNext[T, S]]:
        return (
            cls.is_single_next(x) or
            cls.is_single_next_list(x)
        )
    
    @classmethod
    def is_resolved_next_with_config(cls, x: Any) -> TypeGuard[ResolvedNextWithConfig[T, S]]:
        return (
            cls.is_single_next_with_config(x) or
            cls.is_single_next_with_config_list(x)
        )
    
    @classmethod
    def is_next_callable(cls, x: Any) -> TypeGuard[Callable[[T, S], ResolvedNext[T, S]] | Callable[[T, S], Awaitable[ResolvedNext[T, S]]]]:
        return callable(x) and not (
            cls.is_any_source(x) or # includes Node, START, Exceptions
            x is END
        )
    

    @classmethod
    def is_next(cls, x: Any) -> TypeGuard[Next[T, S]]:
        return (
            cls.is_resolved_next(x) or
            cls.is_next_callable(x)
        )
    
    @classmethod
    def is_next_with_config(cls, x: Any) -> TypeGuard[NextWithConfig[T, S]]:
        return (
            cls.is_resolved_next_with_config(x) or
            cls.is_next_callable(x)
        )

    @classmethod
    def is_single_source(cls, x: Any) -> TypeGuard[SingleSource[T, S]]:
        return (
            isinstance(x, Node) or
            x is START
        )
    
    @classmethod
    def is_single_source_with_config(cls, x: Any) -> TypeGuard[SingleSourceWithConfig[T, S]]:
        return cls.is_node_with_config(x) or cls.is_single_source(x)

    @classmethod
    def is_single_source_list(cls, x: Any) -> TypeGuard[list[SingleSource[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_source(n) for n in cast(list[Any], x))
    
    @classmethod
    def is_single_source_with_config_list(cls, x: Any) -> TypeGuard[list[SingleSourceWithConfig[T, S]]]:
        return isinstance(x, list) and all(cls.is_single_source_with_config(n) for n in cast(list[Any], x))

    @classmethod
    def is_source(cls, x: Any) -> TypeGuard[Source[T, S]]:
        return (
            cls.is_single_source(x) or
            cls.is_single_source_list(x)
        )
    
    @classmethod
    def is_source_with_config(cls, x: Any) -> TypeGuard[SourceWithConfig[T, S]]:
        return (
            cls.is_single_source_with_config(x) or
            cls.is_single_source_with_config_list(x)
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
    def is_error_source(cls, x: Any) -> TypeGuard[ErrorSource[T, S]]:
        return (
            cls.is_single_error_source(x) or
            cls.is_single_error_source_list(x)
        )
    
    @classmethod
    def is_any_single_source(cls, x: Any) -> TypeGuard[SingleSourceWithConfig[T, S] | SingleErrorSource[T, S]]:
        return cls.is_single_source_with_config(x) or cls.is_single_error_source(x)

    @classmethod
    def is_any_single_source_list(cls, x: Any) -> TypeGuard[list[SingleSourceWithConfig[T, S] | SingleErrorSource[T, S]]]:
        return isinstance(x, list) and all(cls.is_any_single_source(n) for n in cast(list[Any], x))
    
    @classmethod
    def is_any_source(cls, x: Any) -> TypeGuard[SourceWithConfig[T, S] | ErrorSource[T, S]]:
        return cls.is_source_with_config(x) or cls.is_error_source(x)

    @classmethod
    def is_branch_join(cls, x: Any) -> TypeGuard[BranchJoin[T, S]]:
        return x is END or cls.is_next(x)


class ErrorConfig(BaseModel):
    """
    Configuration for the error edge.

    Attributes:
        propagate: If the error should be propagated to the next error edge. If False, the error is caught and the graph continues.
    """

    propagate: bool = False

class Edge[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    An edge in a branch.

    Attributes:
        source: The source of the edge.
        next: The unresolved targets of the edge.
    """

    source: SingleSource[T, S]
    next: Next[T, S]
    model_config = ConfigDict(arbitrary_types_allowed=True)

class ErrorEdge[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    An error edge in a branch.

    Attributes:
        source: The source of the error edge.
        next: The unresolved targets of the error edge.
        config: The configuration of the error edge.
    """

    source: SingleErrorSource[T, S]
    next: Next[T, S]
    config: ErrorConfig = Field(default_factory=ErrorConfig)
    model_config = ConfigDict(arbitrary_types_allowed=True)


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
    """

class ErrorEntry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the error edge indexing dictionary of a branch.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
        propagate: If the error should be reraised. If False, the error is caught and the graph continues.
    """

    propagate: bool = Field(default=False)
    # config: ErrorConfig = Field(default_factory=ErrorConfig)


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
