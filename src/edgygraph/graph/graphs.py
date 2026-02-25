
from __future__ import annotations

from typing import cast, Any, Sequence, Hashable
from collections import defaultdict
from collections.abc import Hashable
import asyncio

from ..states import StateProtocol, SharedProtocol
from ..diff import Change, ChangeConflictException, Diff
from ..nodes import END
from .types import NodeTupel, Edge, ErrorEdge, SingleNext
from .hooks import GraphHook
from .branches import Branch



class Graph[T: StateProtocol = StateProtocol, S: SharedProtocol = SharedProtocol]:
    """
    Create and execute a graph defined by a list of edges


    ## Generic Typing Parameters 

    Use protocols or classes that extend **StateProtocol** and **SharedProtocol** or **State** and **Shared** to define the supported state types.

    #### Inheritance with Variance

    With covariance its possible to use nodes that use more specific State and Shared classes as the generic typing parameters. Requires an inheritance structure.

    This is recommended for smaller projects because it needs less boilerplate.

    #### Duck Typing

    For the more flexible approach with better scaling use protocols to define the supported state types. Remember to always extend `typing.Protocol` in the child classes for typing.

    This is recommended for scalable projects where many different state types need to be joined in one graph. See https://github.com/mathisxy/edgynodes/ for an example.

    #### Disable Type Checking

    If you want to disable type checking for the graph, you can use `typing.Any` as generic typing parameters in the graph.

    
    ## Edges

    The edges are defined as a list of tuples, where the first element is the source and the second element reveals the next node.

    #### Formats

    The graph supports different formats for the edges.

    - `(source, target)`: A single edge from source to target.
    - `(START, target)`: A single edge from the start of the graph to target.
    - `(source, END)`: A single edge from source to the end of the graph. It equals to `(source, None)`. It is redundant but can be used for better readability.
    - `([source1, source2], target)`: Multiple edges from source1 and source2 to target.
    - `(source, [target1, target2])`: Multiple edges from source to target1 and target2.
    - `([source1, source2], [target1, target2])`: Multiple edges from source1 and source2 to target1 and target2. This will create 4 edges in total.
    - `(source, lambda st, sh: [target1, target2] if sh.x)`: A dynamic edge from source to target. The function takes the state and the shared state as arguments. It must return a node, a list of nodes, END or None. Async functions are also supported. They are executed sequentially so there are no race conditions.
    - `(source, target, Config(instant=True))`: An instant edge from source to target. The target nodes are collected recursively and executed parallel to the source node. Make sure not to create cycles.
    - `(ValueError, target)`: An error edge from ValueError to target. The edge is traversed if a node, which is executed by an incoming edge located BEFORE this error edge in the edge list, throws a ValueError.
    - `((source, Exception), target)`: An error edge from Exception to target. The edge is traversed if the source node is executed by an incoming edge which is located BEFORE this error edge in the edge list throws an Exception. Source node lists are also supported.
    - `(Exception, target, ErrorConfig(propagate=True))`: If propagate is `True`, the exception is propagated to the next error edges in the edge list. If the exception is not handled by any error edge, it is ultimately raised.


    Attributes:
        edges: A list of edges of compatible nodes that build the graph.
        hooks: A list of graph hook classes. Usable for debugging, logging and custom logic.
    """

    edges: Sequence[Edge[T, S] | ErrorEdge[T, S] | NodeTupel[T, S]]
    hooks: Sequence[GraphHook[T, S]]

    join_registry: dict[SingleNext[T, S], list[Branch[T, S]]]


    @property
    def task_group(self) -> asyncio.TaskGroup:
        if self.tg is None:
            raise RuntimeError("TaskGroup not initialized")
        return self.tg
    
    tg: asyncio.TaskGroup | None = None


    def __init__(self, edges: list[Edge[T, S]], hooks: list[GraphHook[T, S]] | None = None) -> None:
        self.edges = edges
        self.hooks = hooks or []

        self.join_registry = defaultdict(list)


    async def __call__(self, state: T, shared: S) -> tuple[T, S]:
        """
        Run the graph on the given state and shared state.
        """

        # Hook
        for h in self.hooks: await h.on_graph_start(state, shared)

        async with asyncio.TaskGroup() as tg:

            # Initialization
            self.tg = tg
            self.spawn_branch(state, shared, Branch[T, S](graph=self, edges=self.edges))

        state_dict: dict[Hashable, Any] = cast(dict[Hashable, Any], state.model_dump())

        for branch in self.join_registry[END]:

            if branch.result is None:
                raise ValueError(f"Branch result is None: {branch}")

            changes = await branch.result

            Diff.apply_changes(state_dict, changes)

        # Final state
        final_state = state.model_validate(state_dict)

        # Hook
        for h in self.hooks: await h.on_graph_end(final_state, shared)

        return final_state, shared


            
    def spawn_branch(self, state: T, shared: S, branch: Branch[T, S]) -> None:

        branch = Branch[T, S](graph=self, edges=self.edges)

        self.join_registry[branch.join].append(branch)

        self.task_group.create_task(branch(state, shared))



    async def merge_states(self, current_state: T, result_states: list[T]) -> T:
        """
        Merges the result states into the current state.
        First the changes are calculated for each result state.
        Then the changes are checked for conflicts.
        If there are conflicts, a ChangeConflictException is raised.
        The changes are applied in the order of the result states list.

        Args:
            current_state: The current state
            result_states: The result states

        Returns:
            The new merged State instance.

        Raises:
            ChangeConflictException: If there are conflicts in the changes.
        """
            
        result_dicts = [state.model_dump() for state in result_states]
        current_dict = cast(dict[Hashable, Any], current_state.model_dump())
        state_class = type(current_state)

        changes_list: list[dict[tuple[Hashable, ...], Change]] = []

        hooks = self.hooks


        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
        

        # Hook
        for h in hooks: await h.on_merge_start(current_state, result_states, changes_list)


        conflicts = Diff.find_conflicts(changes_list)

        if conflicts:

            # Hook
            for h in hooks: await h.on_merge_conflict(current_state, result_states, changes_list, conflicts)

            raise ChangeConflictException(f"Conflicts detected after parallel execution: {conflicts}")


        for changes in changes_list:
            Diff.apply_changes(current_dict, changes)

        state: T = state_class.model_validate(current_dict)


        # Hook
        for h in hooks: await h.on_merge_end(current_state, result_states, changes_list, state)

        return state
    