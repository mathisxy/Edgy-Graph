from .edges import GraphEdge, START, END
from .nodes import GraphNode
from .states import GraphState
from typing import Type, TypeVar, Generic
from collections import defaultdict

T = TypeVar('T', bound=GraphState)

class GraphExecutor(Generic[T]):

    edges: list[GraphEdge[T]]

    def __init__(self, edges: list[GraphEdge[T]]):
        self.edges = edges

    async def __call__(self, initial_state: T) -> T:
        state: T = initial_state
        current_node: GraphNode[T] | Type[START] = START

        index_dict: dict[GraphNode[T] | Type[START], list[GraphEdge[T]]] = defaultdict(list[GraphEdge[T]])
        for edge in self.edges:
            if isinstance(edge.source, list):
                for source in edge.source:
                    index_dict[source].append(edge)
            else:
                index_dict[edge.source].append(edge)

        while True:
            # Find the edge corresponding to the current node
            edges: list[GraphEdge[T]] = index_dict[current_node]

            if not edges:
                break # END

            # Determine the next node using the edge's next function
            for edge in edges:
                next_node = edge.next(state)

                if next_node == END:
                    continue
                else:
                    assert isinstance(next_node, GraphNode)
                    # Run the current node to update the state
                    state: T = await next_node.run(state)
                    current_node = next_node

        return state