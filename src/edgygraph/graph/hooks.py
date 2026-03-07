from abc import ABC
from collections.abc import Hashable

from ..states import StateProtocol, SharedProtocol
from ..diff import Change
from .branches import Branch
from .types import BranchJoin, NextNode, SingleSource



class GraphHook[T: StateProtocol, S: SharedProtocol](ABC):
    """
    Hook for the graph execution.

    Hooks are called at different stages of the graph execution.
    They can be used to log, modify the state, or perform other actions.
    """

    async def on_graph_start(self, state: T, shared: S) -> None:
        """
        Called when the graph starts.

        Args:
            state: The initial state of the graph.
            shared: The initial shared state of the graph.
        """

        pass


    async def on_step_start(self, state: T, shared: S, nodes: list[NextNode[T, S]]) -> None:
        """
        Called when a step starts.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            nodes: The nodes that will be executed in this step.
        """

        pass


    async def on_step_end(self, state: T, shared: S, nodes: list[NextNode[T, S]]) -> None:
        """
        Called when a step ends.

        It is called after all nodes have been executed and the state has been merged.

        Args:
            state: The updated state of the graph.
            shared: The shared state of the graph.
            nodes: The nodes that were executed in this step.
        """

        pass


    async def on_spawn_branch_start(self, state: T, shared: S, branch: Branch[T, S], trigger: NextNode[T, S], branch_registry: dict[SingleSource[T, S], list[Branch[T, S]]], join_registry: dict[BranchJoin[T, S], list[Branch[T, S]]]):
        """
        Called before a branch is spawned.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            branch: The branch to be spawned.
            branch_registry: The branch registry of the graph.
            source_node: The node that spawned the branch.
        """

        pass

    
    async def on_spawn_branch_end(self, state: T, shared: S, branch: Branch[T, S], trigger: NextNode[T, S], branch_registry: dict[SingleSource[T, S], list[Branch[T, S]]], join_registry: dict[BranchJoin[T, S], list[Branch[T, S]]]):
        """
        Called after a branch is spawned.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            branch: The branch that was spawned.
            branch_registry: The branch registry of the graph.
            trigger: The node that spawned the branch.
        """

        pass



    async def on_merge_start(self, state: T, result_states: list[T], changes: list[dict[tuple[Hashable, ...], Change]]) -> None:
        """
        Called when the merge process starts.
        
        Args:
            state: The old state of the graph.
            result_states: The result states of the nodes.
            changes: The changes that will be applied to the state.
        """

        pass


    async def on_merge_conflict(self, state: T, changes: list[dict[tuple[Hashable, ...], Change]], conflicts: dict[tuple[Hashable, ...], list[Change]]) -> None:
        """
        Called when a merge conflict occurs.
        
        Args:
            state: The old state of the graph.
            changes: The changes that will be applied to the state.
            conflicts: The conflicts that occurred during the merge process.
        """

        pass


    async def on_merge_end(self, state: T, result_states: list[T], changes: list[dict[tuple[Hashable, ...], Change]], merged_state: T) -> None:
        """
        Called when the merge process ends.
        
        Args:
            state: The old state of the graph.
            result_states: The result states of the nodes.
            changes: The changes that have been applied to the state.
            merged_state: The new merged state of the graph.
        """

        pass


    async def on_graph_end(self, state: T, shared: S) -> None:
        """
        Called when the graph execution ends.

        Args:
            state: The final state of the graph.
            shared: The final shared data.
        """

        pass


    async def on_error(self, error: Exception, state: T, shared: S) -> Exception | None:
        """
        Called when an error occurs during the graph execution.

        Args:
            error: The error that occurred.

        Returns:
           The error to raise, or None not to raise an error.
        """

        return error