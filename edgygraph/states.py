from pydantic import BaseModel, Field
from typing import TypeVar, AsyncIterator, Protocol, Generic
from types import TracebackType

class GraphState(BaseModel):
    vars: dict[str, object] = Field(default_factory=dict)


T = TypeVar('T', covariant=True)

class Stream(AsyncIterator[T], Protocol):
    async def aclose(self) -> None: ...

    async def __aenter__(self) -> "Stream[T]":
        return self

    async def __aexit__(
            self, exc_type: type[BaseException] | None, 
            exc: BaseException | None, 
            tb: TracebackType | None
        ) -> None: # Not handling exceptions here -> returns None

        await self.aclose()


class GraphStateStream(GraphState, Generic[T]):
    
    stream: Stream[T] | None = None