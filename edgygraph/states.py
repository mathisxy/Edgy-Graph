from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict, Field
from typing import AsyncIterator
from types import TracebackType
from asyncio import Lock


class Stream[T: object](ABC, AsyncIterator[T]):

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


class State(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=False) # for deep copy


class Shared(BaseModel):
    lock: Lock = Field(default_factory=Lock)
    model_config = ConfigDict(arbitrary_types_allowed=True)