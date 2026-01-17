from abc import ABC, abstractmethod
from .states import State, StateUpdate
from typing import TypeVar, Generic


T = TypeVar('T', bound=State)

class GraphNode(ABC, Generic[T]):
    
    @abstractmethod
    async def run(self, state: T) -> list[StateUpdate[T]]:
        pass