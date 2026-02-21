import pytest
import asyncio
from asyncio import Lock
from collections.abc import Hashable

from edgygraph import Graph, Node, State, Shared, START, END, Config, ErrorConfig
from edgygraph.graphs import Entry, ErrorEntry
from edgygraph.diff import ChangeTypes, Diff, Change, ChangeConflictException


class TestNode(Node[State, Shared]):

    def __init__(self, name: str, output: list[str]):
        self.name = name
        self.output = output
    
    async def __call__(self, state: State, shared: Shared):
        self.output.append(self.name)



@pytest.mark.asyncio
async def test_graph_index_edges():

    output: list[str] = []

    node1 = TestNode("node1", output)
    node2 = TestNode("node2", output)
    node3 = TestNode("node3", output)
    node4 = TestNode("node4", output)


    graph = Graph(edges=[
        (START, node1),
        (node1, node2),
        (node2, node3, Config(instant=True)),
        (node3, END),
        (node3, None),
        (Exception, node3),
        (Exception, node4, ErrorConfig(propagate=True)),
        ((node3, Exception), node4),
        ((node3, Exception), node4, ErrorConfig(propagate=False)),
        (([node3, node4], Exception), node4)
    ])

    assert graph.edge_index == {
        START: [Entry(next=node1, index=0, config=Config())],
        node1: [Entry(next=node2, index=1, config=Config())],
        node2: [Entry(next=node3, index=2, config=Config(instant=True))],
        node3: [Entry(next=END, index=3, config=Config()), Entry(next=None, index=4, config=Config())],
    }
    assert graph.error_edge_index == {
        Exception: [
            ErrorEntry(next=node3, index=5, config=ErrorConfig()),
            ErrorEntry(next=node4, index=6, config=ErrorConfig(propagate=True)),
        ],
        (node3, Exception): [
            ErrorEntry(next=node4, index=7, config=ErrorConfig()),
            ErrorEntry(next=node4, index=8, config=ErrorConfig(propagate=False)),
            ErrorEntry(next=node4, index=9, config=ErrorConfig()),
        ],
        (node4, Exception): [
            ErrorEntry(next=node4, index=9, config=ErrorConfig()),
        ],

    }


### THE FOLLOWING PART IS CURRENTLY MOSTLY WRITTEN BY AI

# ===========================================================================
# Helpers / fixtures
# ===========================================================================

class SimpleState(State):
    value: int = 0
    name: str = ""

class SimpleShared(Shared):
    pass


class IncrementNode(Node[SimpleState, SimpleShared]):
    async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
        state.value += 1

class SetNameNode(Node[SimpleState, SimpleShared]):
    def __init__(self, name: str):
        self.name = name

    async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
        state.name = self.name

class NoOpNode(Node[SimpleState, SimpleShared]):
    async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
        pass

class RaisingNode(Node[SimpleState, SimpleShared]):
    def __init__(self, exc: Exception):
        self.exc = exc

    async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
        raise self.exc

class RecoveryNode(Node[SimpleState, SimpleShared]):
    async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
        state.name = "recovered"


# ===========================================================================
# Tests: Diff
# ===========================================================================

class TestDiffRecursiveDiff:
    def test_no_changes(self):
        d = {"a": 1, "b": 2}
        assert Diff.recursive_diff(d, d) == {}

    def test_updated_scalar(self):
        changes = Diff.recursive_diff({"a": 1}, {"a": 2})
        assert ("a",) in changes
        assert changes[("a",)].type == ChangeTypes.UPDATED
        assert changes[("a",)].old == 1
        assert changes[("a",)].new == 2

    def test_added_key(self):
        changes = Diff.recursive_diff({}, {"x": 42})
        assert ("x",) in changes
        assert changes[("x",)].type == ChangeTypes.ADDED

    def test_removed_key(self):
        changes = Diff.recursive_diff({"x": 42}, {})
        assert ("x",) in changes
        assert changes[("x",)].type == ChangeTypes.REMOVED

    def test_nested_update(self):
        old = {"a": {"b": 1}}
        new = {"a": {"b": 2}}
        changes = Diff.recursive_diff(old, new)
        assert ("a", "b") in changes
        assert changes[("a", "b")].type == ChangeTypes.UPDATED

    def test_nested_add(self):
        old = {"a": {"b": 1}}
        new = {"a": {"b": 1, "c": 3}}
        changes = Diff.recursive_diff(old, new)
        assert ("a", "c") in changes
        assert changes[("a", "c")].type == ChangeTypes.ADDED

    def test_equal_scalars_no_change(self):
        assert Diff.recursive_diff(5, 5) == {}

    def test_unequal_scalars(self):
        changes = Diff.recursive_diff(5, 10)
        assert () in changes
        assert changes[()].type == ChangeTypes.UPDATED


class TestDiffFindConflicts:
    def test_no_conflict_single_change(self):
        c: list[dict[tuple[Hashable, ...], Change]] = [{("a",): Change(type=ChangeTypes.UPDATED, old=1, new=2)}]
        assert Diff.find_conflicts(c) == {}

    def test_no_conflict_disjoint(self):
        c: list[dict[tuple[Hashable, ...], Change]] = [
            {("a",): Change(type=ChangeTypes.UPDATED, old=1, new=2)},
            {("b",): Change(type=ChangeTypes.UPDATED, old=3, new=4)},
        ]
        assert Diff.find_conflicts(c) == {}

    def test_conflict_same_key(self):
        key = ("value",)
        c: list[dict[tuple[Hashable, ...], Change]] = [
            {key: Change(type=ChangeTypes.UPDATED, old=0, new=1)},
            {key: Change(type=ChangeTypes.UPDATED, old=0, new=2)},
        ]
        conflicts = Diff.find_conflicts(c)
        assert key in conflicts
        assert len(conflicts[key]) == 2

    def test_empty_changes(self):
        assert Diff.find_conflicts([]) == {}


class TestDiffApplyChanges:
    def test_apply_update(self):
        target: dict[Hashable, int] = {"a": 1}
        changes: dict[tuple[Hashable, ...], Change] = {("a",): Change(type=ChangeTypes.UPDATED, old=1, new=99)}
        Diff.apply_changes(target, changes)
        assert target["a"] == 99

    def test_apply_add(self):
        target: dict[Hashable, int] = {}
        changes: dict[tuple[Hashable, ...], Change] = {("x",): Change(type=ChangeTypes.ADDED, old=None, new=7)}
        Diff.apply_changes(target, changes)
        assert target["x"] == 7

    def test_apply_remove(self):
        target: dict[Hashable, int] = {"z": 5}
        changes: dict[tuple[Hashable, ...], Change] = {("z",): Change(type=ChangeTypes.REMOVED, old=5, new=None)}
        Diff.apply_changes(target, changes)
        assert "z" not in target

    def test_apply_nested_update(self):
        target: dict[Hashable, dict[str, int]]    = {"a": {"b": 1}}
        changes: dict[tuple[Hashable, ...], Change] = {("a", "b"): Change(type=ChangeTypes.UPDATED, old=1, new=42)}
        Diff.apply_changes(target, changes)
        assert target["a"]["b"] == 42

    def test_remove_missing_key_raises(self):
        target: dict[Hashable, int] = {}
        changes: dict[tuple[Hashable, ...], Change] = {("missing",): Change(type=ChangeTypes.REMOVED, old=1, new=None)}
        with pytest.raises(KeyError):
            Diff.apply_changes(target, changes)


# ===========================================================================
# Tests: Graph – basic execution
# ===========================================================================

inc = IncrementNode()

class TestGraphBasicExecution:
    @pytest.fixture
    def inc(self):
        return inc

    @pytest.fixture
    def noop(self):
        return NoOpNode()

    def test_single_node_increments_value(self):
        state = SimpleState(value=0)
        shared = SimpleShared()
        g = Graph[SimpleState, SimpleShared](edges=[(START, inc), (inc, END)])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 1

    def test_chain_of_two_nodes(self):
        n1 = IncrementNode()
        n2 = IncrementNode()
        state = SimpleState(value=0)
        shared = SimpleShared()
        g = Graph(edges=[(START, n1), (n1, n2), (n2, END)])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 2

    def test_empty_graph_returns_unchanged_state(self):
        state = SimpleState(value=42)
        shared = SimpleShared()
        g = Graph[SimpleState, SimpleShared](edges=[(START, END)])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 42

    def test_no_edges_from_start_returns_unchanged(self):
        state = SimpleState(value=7)
        shared = SimpleShared()
        g = Graph[SimpleState, SimpleShared](edges=[])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 7

    def test_shared_is_same_object(self):
        state = SimpleState()
        shared = SimpleShared()
        g = Graph[SimpleState, SimpleShared](edges=[(START, inc), (inc, END)])
        _, result_shared = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_shared is shared


# ===========================================================================
# Tests: Graph – conditional edges
# ===========================================================================

class TestGraphConditionalEdges:
    def test_conditional_next_based_on_state(self):
        inc = IncrementNode()
        noop = NoOpNode()

        def router(state: SimpleState, shared: SimpleShared):
            return noop if state.value > 0 else END

        state = SimpleState(value=1)
        shared = SimpleShared()
        g = Graph(edges=[(START, inc), (inc, router)])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        # noop ran (value incremented once by inc, noop does nothing)
        assert result_state.value == 2

    def test_conditional_returns_end(self):
        inc = IncrementNode()

        def router(state: SimpleState, shared: SimpleShared):
            return END

        state = SimpleState(value=0)
        shared = SimpleShared()
        g = Graph(edges=[(START, inc), (inc, router)])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 1

    def test_async_conditional_next(self):
        inc = IncrementNode()
        noop = NoOpNode()

        async def async_router(state: SimpleState, shared: SimpleShared):
            return noop

        state = SimpleState(value=0)
        shared = SimpleShared()
        g = Graph(edges=[(START, inc), (inc, async_router), (noop, END)])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 1


# ===========================================================================
# Tests: Graph – parallel execution and merge
# ===========================================================================

class TestGraphParallelExecution:
    def test_parallel_non_conflicting_changes(self):
        """Two nodes each modify a different field – should merge without conflict."""

        class SetValue(Node[SimpleState, SimpleShared]):
            async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
                state.value = 99

        class SetName(Node[SimpleState, SimpleShared]):
            async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
                state.name = "hello"

        sv = SetValue()
        sn = SetName()
        join = NoOpNode()

        state = SimpleState()
        shared = SimpleShared()
        g = Graph(edges=[
            (START, [sv, sn]),
            ([sv, sn], join),
            (join, END),
        ])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 99
        assert result_state.name == "hello"

    def test_parallel_conflicting_changes_raise(self):
        """Both nodes modify the same field – should raise ChangeConflictException."""

        class SetValue1(Node[SimpleState, SimpleShared]):
            async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
                state.value = 1

        class SetValue2(Node[SimpleState, SimpleShared]):
            async def __call__(self, state: SimpleState, shared: SimpleShared) -> None:
                state.value = 2

        sv1 = SetValue1()
        sv2 = SetValue2()

        state = SimpleState()
        shared = SimpleShared()
        g = Graph(edges=[
            (START, [sv1, sv2]),
        ])
        with pytest.raises((ChangeConflictException, ExceptionGroup)):
            asyncio.get_event_loop().run_until_complete(g(state, shared))


# ===========================================================================
# Tests: Graph – error edges
# ===========================================================================

class TestGraphErrorEdges:
    def test_error_edge_by_exception_type(self):
        raiser = RaisingNode(ValueError("boom"))
        recovery = RecoveryNode()

        state = SimpleState()
        shared = SimpleShared()
        g = Graph(edges=[
            (START, raiser),
            (ValueError, recovery),
            (recovery, END),
        ])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.name == "recovered"

    def test_error_edge_by_node_and_exception_type(self):
        raiser = RaisingNode(RuntimeError("fail"))
        recovery = RecoveryNode()

        state = SimpleState()
        shared = SimpleShared()
        g = Graph(edges=[
            (START, raiser),
            ((raiser, RuntimeError), recovery),
            (recovery, END),
        ])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.name == "recovered"

    def test_unhandled_error_propagates(self):
        raiser = RaisingNode(TypeError("unhandled"))

        state = SimpleState()
        shared = SimpleShared()
        g = Graph(edges=[
            (START, raiser),
        ])
        with pytest.raises(ExceptionGroup):
            asyncio.get_event_loop().run_until_complete(g(state, shared))

    def test_wrong_exception_type_not_caught(self):
        """ValueError handler should NOT catch a RuntimeError."""
        raiser = RaisingNode(RuntimeError("not a value error"))

        state = SimpleState()
        shared = SimpleShared()
        g = Graph(edges=[
            (START, raiser),
            (ValueError, RecoveryNode()),
        ])
        with pytest.raises(ExceptionGroup):
            asyncio.get_event_loop().run_until_complete(g(state, shared))


# ===========================================================================
# Tests: Graph – instant edges
# ===========================================================================

class TestGraphInstantEdges:
    def test_instant_node_runs_in_same_step(self):
        """An instant node should be included in the next step resolution."""
        inc = IncrementNode()
        noop = NoOpNode()

        state = SimpleState(value=0)
        shared = SimpleShared()
        g = Graph(edges=[
            (START, inc),
            (inc, noop, Config(instant=True)),
            (noop, END),
        ])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 1  # inc ran; noop is instant and runs too


# ===========================================================================
# Tests: Graph – multi-source edges
# ===========================================================================

class TestGraphMultiSourceEdges:
    def test_list_source_registers_for_each_node(self):
        n1 = IncrementNode()
        n2 = IncrementNode()
        n3 = NoOpNode()
        join = NoOpNode()

        state = SimpleState(value=0)
        shared = SimpleShared()

        g = Graph(edges=[
            (START, [n1, n3]),
            (n3, n2),
            ([n1, n2], join),
            (join, END),
        ])
        result_state, _ = asyncio.get_event_loop().run_until_complete(g(state, shared))
        assert result_state.value == 2  # both increments applied


# ===========================================================================
# Tests: Node / State basics
# ===========================================================================

class TestNodeAndState:
    def test_node_is_abstract(self):
        with pytest.raises(TypeError):
            Node()  # type: ignore

    def test_state_deep_copy_is_independent(self):
        s = SimpleState(value=5)
        s2 = s.model_copy(deep=True)
        s2.value = 99
        assert s.value == 5

    def test_state_model_dump_round_trip(self):
        s = SimpleState(value=3, name="test")
        d = s.model_dump()
        s2 = SimpleState.model_validate(d)
        assert s2.value == 3
        assert s2.name == "test"

    def test_shared_has_lock(self):
        sh = SimpleShared()
        assert isinstance(sh.lock, Lock)

    
    