from abc import ABC, abstractmethod
from .states import GraphState


class GraphNode(ABC):
    
    @abstractmethod
    def run(self, state: GraphState) -> GraphState:
        pass