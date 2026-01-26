from abc import ABC, abstractmethod
from .states import State, Shared

class Node[T: State = State, S: Shared = Shared](ABC):
    
    @abstractmethod
    async def run(self, state: T, shared: S) -> None:
        pass