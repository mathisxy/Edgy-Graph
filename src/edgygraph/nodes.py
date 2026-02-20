from abc import ABC, abstractmethod

from .states import StateProtocol, SharedProtocol

class Node[T: StateProtocol = StateProtocol, S: SharedProtocol = SharedProtocol](ABC):
    """
    Represents a node in the graph.

    The generic types define the type of state and shared state that the node expects.
    Due to variance the node can also take any subtype of the state and shared state.

    The node must implement the `__call__` method to run the node.
    """

    cancel_other_on_finish: bool = False

    
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


    # @classmethod
    # def from_func(cls: type["Node[T, S]"], func: Callable[[T, S], None | Awaitable[None]]) -> "Node[T, S]":
    #     """
    #     Create a node from a function.

    #     Convenience method to create a node from a function.
    #     The function can be synchronous or asynchronous.

    #     Args:
    #         func: The function for the node to execute.
    #     """
    #     class FunctionNode(Node[T, S]):

    #         async def __call__(self, state: T, shared: S) -> None:

    #             result = func(state, shared)

    #             if inspect.isawaitable(result):
    #                 await result

    #     return FunctionNode()



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