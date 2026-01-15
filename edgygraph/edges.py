from typing import Callable, Type, TypeVar, Generic
from .states import GraphState
from .nodes import GraphNode


class START:
    pass

class END:
    pass


T = TypeVar('T', bound=GraphState)

class GraphEdge(Generic[T]):

    source: GraphNode[T] | Type[START] | list[GraphNode[T] | Type[START]]
    next: Callable[[T], GraphNode[T] | Type[END]]

    def __init__(self, source: GraphNode[T] | Type[START] | list[GraphNode[T] | Type[START]], next: Callable[[T], GraphNode[T] | Type[END]]):
        self.source = source
        self.next = next
