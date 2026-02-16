from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict, Field
from typing import AsyncIterator, Protocol, Any, Self
from types import TracebackType
from asyncio import Lock


class PydanticModel(Protocol):
    """Minimal Protocol for Pydantic BaseModel Operations"""
    
    def model_dump(self) -> dict[str, Any]: ...
    
    def model_copy(self, *, deep: bool = False) -> Self: ...
    
    @classmethod
    def model_validate(cls, obj: Any) -> Self: ...


class StateProtocol(PydanticModel):
    pass


class SharedProtocol(PydanticModel):
    lock: Lock


class State(BaseModel):
    """
    Holds variables for the nodes of serializable types. Arbitrary types are not allowed.

    The state is copied for each parallel node and merged after the node has finished.
    Therefore, the state is not shared between nodes and operations are safe.

    Parallel changes of the same variable will be detected as a conflict and raise an error.

    Implements pydantic's BaseModel.
    """
    model_config = ConfigDict(arbitrary_types_allowed=False) # for deep copy


class Shared(BaseModel):
    """
    Holds shared variables for the nodes of any type.

    The shared state is shared between all parallel nodes and operations are not safe without using the Lock.
    The lock can be accessed via `shared.lock`.

    Implements pydantic's BaseModel with arbitrary types allowed.
    """
    lock: Lock = Field(default_factory=Lock)
    model_config = ConfigDict(arbitrary_types_allowed=True)


class StateAttribute(BaseModel):
    """
    Holds variables of serializable types for the nodes. Arbitrary types are not allowed.

    Simplifies composition in states.
    """
    model_config = ConfigDict(arbitrary_types_allowed=False) # for deep copy

class SharedAttribute(BaseModel):
    """
    Holds shared variables of any type for the nodes.

    Simplifies composition in shared states.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)


class Stream[T: object](ABC, AsyncIterator[T]):
    """
    Standardized wrapper interface for streams of data.

    Set the type of the data that the stream will yield with the generic type parameter `T`.

    Implements an async iterator and async context manager.
    """

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