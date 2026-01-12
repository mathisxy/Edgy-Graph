from abc import ABC, abstractmethod
from .states import GraphState
from typing import TypeVar, Generic


T = TypeVar('T', bound=GraphState)

class GraphNode(ABC, Generic[T]):
    
    @abstractmethod
    def run(self, state: T) -> T:
        pass