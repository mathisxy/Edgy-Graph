from typing import Callable, Type
from .states import GraphState
from .nodes import GraphNode


class START:
    pass

class END:
    pass


class GraphEdge:

    source: GraphNode | Type[START]
    next: Callable[[GraphState], GraphNode | Type[END]]

    def __init__(self, source: GraphNode | Type[START], next: Callable[[GraphState], GraphNode | Type[END]]):
        self.source = source
        self.next = next
