from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ConfigDict
from typing import TypeVar, AsyncIterator, Generic, Protocol
from types import TracebackType


T = TypeVar('T', covariant=True, default=object)

class Stream(ABC, AsyncIterator[T]):

    @abstractmethod
    async def aclose(self) -> None:
        pass

    @abstractmethod
    async def __anext__(self) -> T:
        pass

    async def __aenter__(self) -> "Stream[T]":
        return self

    async def __aexit__(
            self, exc_type: type[BaseException] | None, 
            exc: BaseException | None, 
            tb: TracebackType | None
        ) -> None: # Not handling exceptions here -> returns None

        await self.aclose()


class State(BaseModel, Generic[T]):
    vars: dict[str, object] = Field(default_factory=dict)
    streams: dict[str, 'Stream[T]'] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


S = TypeVar('S', bound=State, contravariant=True)

class StateUpdate(Protocol, Generic[S]):
    def __call__(self, s: S) -> None: ...