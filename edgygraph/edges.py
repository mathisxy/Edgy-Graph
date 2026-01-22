from typing import Callable, Type, TypeVar, Generic, Awaitable
from .states import State, Shared
from .nodes import Node


class START:
    pass

class END:
    pass


T = TypeVar('T', bound=State)
S = TypeVar('S', bound=Shared)

class Edge(Generic[T, S]):

    source: Node[T, S] | Type[START] | list[Node[T, S] | Type[START]]
    next: Callable[[T, S], Node[T, S] | Type[END] | Awaitable[Node[T, S] | Type[END]]]

    def __init__(self, source: Node[T, S] | Type[START] | list[Node[T, S] | Type[START]], next: Callable[[T, S], Node[T, S] | Type[END] | Awaitable[Node[T, S] | Type[END]]]):
        self.source = source
        self.next = next
