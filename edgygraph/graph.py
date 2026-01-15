from .edges import GraphEdge, START, END
from .nodes import GraphNode
from .states import GraphState
from typing import Type, TypeVar, Generic

T = TypeVar('T', bound=GraphState)

class GraphExecutor(Generic[T]):

    edges: list[GraphEdge[T]]

    def __init__(self, edges: list[GraphEdge[T]]):
        self.edges = edges

    async def __call__(self, initial_state: T) -> T:
        state = initial_state
        current_node: GraphNode[T] | Type[START] = START

        index_dict: dict[GraphNode[T] | Type[START], GraphEdge[T]] = {edge.source: edge for edge in self.edges}

        while True:
            # Find the edge corresponding to the current node
            edge: GraphEdge[T] = index_dict[current_node]
            # Determine the next node using the edge's next function
            next_node = edge.next(state)

            if next_node == END:
                break
            else:
                assert isinstance(next_node, GraphNode)
                # Run the current node to update the state
                state: GraphState = await next_node.run(state)
                current_node = next_node

        return state