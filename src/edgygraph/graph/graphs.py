
from __future__ import annotations

from typing import cast, Any, Hashable, Callable
from collections import defaultdict
from collections.abc import Hashable, Sequence
import asyncio
import inspect

from ..states import StateProtocol, SharedProtocol
from ..diff import Change, ChangeConflictException, Diff
from ..nodes import Node, END, START
from .types import SingleNext, NextNode, ErrorEntry, SingleErrorSource, Entries, BranchContainer, SingleSource, Source, Types, ResolvedNext, Join
from .hooks import GraphHook
from .branches import Branch



class Graph[T: StateProtocol = StateProtocol, S: SharedProtocol = SharedProtocol]:
    """
    Create and execute a graph defined by a list of edges


    ## Generic Typing Parameters 

    Use protocols or classes that extend **StateProtocol** and **SharedProtocol** or **State** and **Shared** to define the supported state types.

    ### Inheritance with Variance

    With covariance its possible to use nodes that use more specific State and Shared classes as the generic typing parameters. Requires an inheritance structure.

    This is recommended for smaller projects because it needs less boilerplate.

    ### Duck Typing

    For the more flexible approach with better scaling use protocols to define the supported state types. Remember to always extend `typing.Protocol` in the child classes for typing.

    This is recommended for scalable projects where many different state types need to be joined in one graph. See [edgynodes](https://github.com/mathisxy/edgynodes/) for an example.

    ### Disable Type Checking

    If you want to disable type checking for the graph, you can use `typing.Any` as generic typing parameters in the graph.

    
    ## Edges

    The edges are defined as a list of tuples, where the first element is the source and the second element reveals the next node.

    ### Branches

    The edges are contained in branches. A branch is a tuple with edges and a join parameter at the end. 

    #### Spawning

    A branch is spawned when the source of the first edge of the branch is triggered.

    In this example it would be on `START`:

    ```python
    edges=[(
       (START, node1),
       (node1, node2),
       END
    )]
    ```

    In this example it would be on `node1` and on `node2` each:

    ```python
    edges=[(
       ([node1, node2], node3),
       node4
    )]
    ```

    #### Joining

    The join parameter can be of the following types:

    - `None`: The branch will not be joined.
    - `END`: The branch will be joined at the end of the graph.
    - A node instance: The branch will be joined directly before the given node is executed in any branch into this branch.

    The process of joining describes waiting for the branches to finish wich aim to join and then applying the changes to the state of the whole finished branch to the state of the joining branch.

    ### Formats

    A branch supports different formats for the edges.

    - `(source, target)`: A single edge from source to target.
    - `(START, target)`: A single edge from the start of the graph to target.
    - `(source, None)`: A single edge from source to no target. `END` is not allowed here, since it would be redundant. It is only allowed in join parameters to distinct join at the end of the graph (`END`) from not joining (`None`)
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


    @property
    def task_group(self) -> asyncio.TaskGroup:
        if self.tg is None:
            raise RuntimeError("TaskGroup not initialized")
        return self.tg
    
    tg: asyncio.TaskGroup | None = None


    def __init__(self, 
            edges: Sequence[BranchContainer[T, S]], 
            hooks: Sequence[GraphHook[T, S]] | None = None
        ) -> None:

        self.edges = edges
        self.hooks = hooks or []

        self.branch_registry: dict[SingleSource[T, S], list[Branch[T, S]]] = defaultdict(list)
        self.join_registry: dict[Join[T, S], list[Branch[T, S]]] = defaultdict(list)

        self.index_branches()

    def index_branches(self) -> None:
        for branch_container in self.edges:

            source = branch_container[0][0]

            if Types[T, S].is_single_source(source):

                branch = Branch[T, S](branch_container[:-1], source, branch_container[-1])
                self.branch_registry[source].append(branch)

            elif Types[T, S].is_single_source_sequence(source):

                for start_node in source:

                    branch = Branch[T, S](branch_container[:-1], start_node, branch_container[-1])
                    self.branch_registry[start_node].append(branch)

            else:
                raise ValueError(f"Invalid source type: {source}")

    async def __call__(self, state: T, shared: S) -> tuple[T, S]:
        """
        Run the graph on the given state and shared state.
        """

        # Hook
        for h in self.hooks: await h.on_graph_start(state, shared)

        async with asyncio.TaskGroup() as tg:

            # Initialization
            self.tg = tg

            for branch in self.branch_registry[START]:
                self.spawn_branch(state, shared, branch)

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
    


    async def run_branch(self, state: T, shared: S, branch: Branch[T, S]) -> None:
        """
        Execute the branch based on the edges

        Args:
            state: State of the first generic type of the graph or a subtype
            shared: Shared of the second generic type of the graph or a subtype

        Returns:
            New State instance and the same Shared instance
        """

        branch.result = asyncio.Future()

        initial_state = state.model_copy(deep=True)

        try:
            
            next_nodes: list[NextNode[T, S]] = await self.get_next(state, shared, branch.start, branch)

            while next_nodes:

                # Hook
                for h in self.hooks: await h.on_step_start(state, shared, next_nodes)

                # Run parallel
                result_states: list[T] = []

                await self.spawn_branches(state, shared, next_nodes)

                state = await self.join_branches(state, next_nodes)

                try:

                    async with asyncio.TaskGroup() as tg:
                        for node in next_nodes:
                            
                            state_copy: T = state.model_copy(deep=True)
                            result_states.append(state_copy)

                            tg.create_task(self.node_wrapper(state_copy, shared, node))

                    # Merge
                    state = await self.merge_states(state, result_states)


                except ExceptionGroup as eg:

                    print("ERROR")
                    print(eg)

                    # Hook
                    for h in self.hooks: await h.on_step_end(state, shared, next_nodes)
                    
                    next_nodes = await self.get_next_from_error(state, shared, eg, branch)
                    
                else:

                    # Hook
                    for h in self.hooks: await h.on_step_end(state, shared, next_nodes)

                    next_nodes = await self.get_next(state, shared, [n.node for n in next_nodes], branch)
        

        except Exception as e:
            
            # Hook
            for h in self.hooks:
                e = await h.on_error(e, state, shared)
                if e is None: 
                    break
            
            if e:
                raise e

        branch.result.set_result(Diff.recursive_diff(initial_state.model_dump(), state.model_dump()))

        

    async def node_wrapper(self, state: T, shared: S, node: NextNode[T, S]):
        """
        Wrapper for the nodes to catch exceptions and add the node to the exception with the key: `source_node`.
        
        This is used to determine the node that caused the exception.
        This is used in the `get_next_nodes_from_error` method to determine the next nodes to execute.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            node: The node to execute.
        """

        try:
            await node.node(state, shared)

        except Exception as e:
            e.source_node = node # type: ignore
            raise e



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

        changes_list: list[dict[tuple[Hashable, ...], Change]] = []


        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
        

        # Hook
        for h in self.hooks: await h.on_merge_start(current_state, result_states, changes_list)


        state = await self.apply_changes(current_state, changes_list)


        # Hook
        for h in self.hooks: await h.on_merge_end(current_state, result_states, changes_list, state)

        return state
    

    async def apply_changes(self, state: T, changes: list[dict[tuple[Hashable, ...], Change]]) -> T:
        """
        Apply changes to the state.

        Args:
            state: The current state.
            changes: A list of changes to apply.

        Raises:
            ChangeConflictException: If there are conflicts in the changes.
        """

        state_dict = cast(dict[Hashable, Any], state.model_dump())
        conflicts = Diff.find_conflicts(changes)

        if conflicts:

            # Hook
            for h in self.hooks: await h.on_merge_conflict(state, changes, conflicts)

            raise ChangeConflictException(f"Conflicts detected: {conflicts}")
        
        
        for change in changes:
            Diff.apply_changes(state_dict, change)

        return type(state).model_validate(state_dict)
    

    async def spawn_branches(self, state: T, shared: S, next_nodes: list[NextNode[T, S]]) -> None:
        """
        Spawn branches based on the next nodes.
    
        Args:
            state: The state of the graph.
            next_nodes: The source nodes of the branches to execute.
        """

        for node in next_nodes:
            for branch in self.branch_registry[node.node]:

                for h in self.hooks: await h.on_spawn_branch_start(state, shared, branch, node, self.branch_registry, self.join_registry)

                self.spawn_branch(state, shared, branch)

                for h in self.hooks: await h.on_spawn_branch_end(state, shared, branch, node, self.branch_registry, self.join_registry)
    
    
            
    def spawn_branch(self, state: T, shared: S, branch: Branch[T, S]) -> None:

        self.join_registry[branch.join].append(branch)

        self.task_group.create_task(self.run_branch(state, shared, branch))

    async def join_branches(self, state: T, next_nodes: list[NextNode[T, S]]) -> T:
        """
        Join all branches that join on one of the next nodes.

        Args:
            state: The state of the graph.
            next_nodes: The next nodes to execute.

        Returns:
            The merged state of the graph after joining the branches.
        """

        changes: list[dict[tuple[Hashable, ...], Change]] = []

        for node in next_nodes:
            for branch in self.join_registry[node.node]:

                if branch.result is None:
                    raise ValueError(f"Branch {branch} has no result")

                changes.append(await branch.result)

                self.join_registry[node.node].remove(branch)

        state = await self.apply_changes(state, changes)

        return state
    


    async def get_next(self, state: T, shared: S, current_nodes: Source[T, S], branch: Branch[T, S]) -> list[NextNode[T, S]]:
        """
        Get the next nodes to run based on the current nodes and the graph's edges.

        Callable edges are called with the state and shared state.

        Args:
            state: The current state
            shared: The shared state
            current_nodes: The current nodes

        Returns:
           The list of the next nodes including their edges that they were reached by.
        """

        next_list: list[NextNode[T, S]] = []

        if Types[T, S].is_single_source_sequence(current_nodes):
            for current_node in current_nodes:
                next_list.extend(
                    await self.resolve_entries(state, shared, branch.edge_index[current_node])
                )

        elif Types[T, S].is_single_source(current_nodes):
            next_list.extend(
                await self.resolve_entries(state, shared, branch.edge_index[current_nodes])
            )
        
        else:
            raise ValueError(f"Invalid current_nodes type: {type(current_nodes)}")


        # Instant nodes
        current_instant_next_list: list[NextNode[T, S]] = []

        while True:

            current_entries = [
                entry
                for next in current_instant_next_list 
                for entry in branch.edge_index[next.node]
                if entry.config.instant
            ]
            
            if not current_entries:
                break
            
            current_instant_next_list = await self.resolve_entries(state, shared, current_entries)
            next_list.extend(current_instant_next_list)


        return next_list
    


    async def get_next_from_error(self, state: T, shared: S, eg: ExceptionGroup, branch: Branch[T, S]) -> list[NextNode[T, S]]:
        """
        Get the next nodes to execute from the error.

        If exceptions in the group dont have the key `source_node` they will be reraised as an Exception group.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.
            eg: The exception group with all errors to get the next nodes from.

        Returns:
            The next nodes to execute.
        """

        next_nodes: list[NextNode[T, S]] = []
        unhandled: list[Exception] = []

        for e in eg.exceptions:            

            print(e)

            source_node: NextNode[T, S] | None = getattr(e, "source_node", None)

            if not isinstance(source_node, NextNode):
                unhandled.append(e)
                continue

            entries: list[ErrorEntry[T, S]] = []

            for key in branch.error_edge_index.keys():
                
                if self.match_error(e, key, source_node):
                    entries.extend(branch.error_edge_index[key])

            entries.sort(key=lambda x: x.index)
            for entry in entries:
                if entry.index > source_node.reached_by.index: # If the error entry is after the node that raised the error
                    next_nodes.extend(
                        await self.resolve_entries(state, shared, [entry])
                    )

                    if not entry.config.propagate:
                        break
            else: # Not consumed
                unhandled.append(e)

        
        if unhandled:
            raise ExceptionGroup("Unhandled node exceptions", unhandled)

        return next_nodes
    


    def match_error(self, e: Exception, source: SingleErrorSource[T, S], source_node: NextNode[T, S]) -> bool:

        match source:
            case type(): # Exception type
                return issubclass(type(e), source)
            case (node, error_type): # (Node, Exception type)
                return node == source_node.node and isinstance(e, error_type)


    async def resolve_entries(self, state: T, shared: S, entries: Sequence[Entries[T, S]]) -> list[NextNode[T, S]]:
    
        return [
            next_node
            for entry in entries
            for next_node in await self.resolve_entry(state, shared, entry)
        ]


    async def resolve_entry(self, state: T, shared: S, entry: Entries[T, S]) -> list[NextNode[T, S]]:
        """
        Resolve the next to nodes.

        Make sure to ONLY call this method ONCE per execution of the corresponding edge.
    
        Args:
            state: The current state.
            shared: The shared state.
        
        Returns:
            The resolved nodes.
        """

        next = entry.next

        if not Types[T, S].is_resolved_next(next):
            if not Types[T, S].is_next(next):
                raise ValueError(f"Invalid next type: {type(next)}")
            
            next = cast(Callable[[T, S], ResolvedNext[T, S]], next)
            next = next(state, shared)

            if inspect.isawaitable(next):
                next = await next
        
        return [
            NextNode[T, S](node=node, reached_by=entry)
            for node in self.get_next_nodes(next)
        ]
    


    def get_next_nodes(self, next: ResolvedNext[T, S]) -> list[Node[T, S]]:

        next_nodes: list[Node[T, S]] = []

        def match(x: SingleNext[T, S]) -> None:

            match x:

                case None:
                    pass
                
                case Node():
                    next_nodes.append(x)

                case _:
                    raise ValueError(f"Invalid next type: {type(x)}")

        
        if Types[T, S].is_single_next_sequence(next):
            for x in next:
                match(x)

        elif Types[T, S].is_single_next(next):
            match(next)

        else:
            raise ValueError(f"Invalid next type: {type(next)}")

        return next_nodes
                    
                    
    

