from __future__ import annotations
from abc import ABC, abstractmethod
from copy import copy
from pydantic import BaseModel
from typing import Literal
from importlib import metadata

from .states import StateProtocol, SharedProtocol

class Node[T: StateProtocol = StateProtocol, S: SharedProtocol = SharedProtocol](ABC):
    """
    Represents a node in the graph.

    The generic types define the type of state and shared state that the node expects.
    Due to variance or the usage of protocols, the node can also take any subtype of the state and shared state.

    The node must implement the `__call__` method to run the node.
    """

    dependencies: list[str] = []
    """The pip dependencies of the node (python packages). On initialization of the node a check is performed if the dependencies are installed with importlib. If not an error is raised."""

    @classmethod
    def check_dependencies(cls) -> None:
        """
        Checks if the dependencies of the node are installed.
        """
        for dependency in cls.dependencies:
            try:
                metadata.version(dependency)
            except metadata.PackageNotFoundError:
                raise ImportError(f"Dependency {dependency} not found but required by node {cls.__name__}. Please install it with pip install {dependency}")


    def __init__(self) -> None:
        self.check_dependencies()

    
    @abstractmethod
    async def __call__(self, state: T, shared: S) -> None:
        """
        Runs the node with the given state and shared state from the graph.

        Operations on the state are merged in the graphs state after the node has finished.
        Therefore, the state is not shared between nodes and operations are safe.

        Operations on the shared state are reflected in all parallel running nodes.
        Therefore, the shared state is shared between nodes and operations are not safe without using the Lock.
        The lock can be accessed via `shared.lock`.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.

        Returns:
            None. The instance references of the arguments are used in the graph to enable variance.
        """
        pass

    
    def copy(self) -> Node[T, S]:
        """
        Creates a copy of the node. This can be used when the same node is used multiple times in the graph.

        Returns:
            A copy of the node.
        """
        return copy(self)
    

    def __neg__(self) -> tuple[Node[T, S], NodeConfig]:
        """
        Negates the node. This can be used to indicate that the node should only be used as a next and not as a source.
        """
        return (self, NodeConfig(operator="neg"))

    def __pos__(self) -> tuple[Node[T, S], NodeConfig]:
        """
        Positivizes the node. This can be used to indicate that the node should only be used as a source and not as a next.
        """
        return (self, NodeConfig(operator="pos"))

class START:
    """
    Represents a start node
    """
    pass

class END:
    """
    Represents an end node
    """
    pass


class NodeConfig(BaseModel):
    """
    Represents the configuration of a node.
    """
    
    operator: Literal["pos", "neg", None] = None

    @property
    def only_next(self) -> bool:
        return self.operator == "neg"
    
    @property
    def only_source(self) -> bool:
        return self.operator == "pos"