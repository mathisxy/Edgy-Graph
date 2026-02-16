from .states import StateProtocol as State, SharedProtocol as Shared

from abc import ABC, abstractmethod


class Node[T: State = State, S: Shared = Shared](ABC):
    """
    Represents a node in the graph.

    The generic types define the type of state and shared state that the node expects.
    Due to variance the node can also take any subtype of the state and shared state.

    The node must implement the `__call__` method to run the node.
    """
    
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