from abc import ABC
from collections.abc import Hashable

from .states import StateProtocol as State, SharedProtocol as Shared
from .diff import Change, ChangeTypes
from .types import NextNode

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.pretty import Pretty
from rich.rule import Rule
from rich.tree import Tree


class GraphHook[T: State, S: Shared](ABC):
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


    async def on_merge_start(self, state: T, result_states: list[T], changes: list[dict[tuple[Hashable, ...], "Change"]]) -> None:
        """
        Called when the merge process starts.
        
        Args:
            state: The old state of the graph.
            result_states: The result states of the nodes.
            changes: The changes that will be applied to the state.
        """

        pass


    async def on_merge_conflict(self, state: T, result_states: list[T], changes: list[dict[tuple[Hashable, ...], Change]], conflicts: dict[tuple[Hashable, ...], list["Change"]]) -> None:
        """
        Called when a merge conflict occurs.
        
        Args:
            state: The old state of the graph.
            result_states: The result states of the nodes.
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



class InteractiveDebugHook[T: State, S: Shared](GraphHook[T, S]):
    """
    A hook that prints the state and shared data at each step of the graph execution.

    This hook is useful for debugging purposes.
    """

    console: Console

    def __init__(self) -> None:
        self.console = Console()

    def _pause(self):
        self.console.print(Rule(style="dim"))
        input("Press Enter to continue...")

    async def on_graph_start(self, state: T, shared: S):
        self.console.print(Rule("[bold magenta]Graph Execution Started", style="magenta"))
        
        # State and Shared side by side
        state_view = Panel(Pretty(state), title="Initial State", border_style="blue")
        shared_view = Panel(Pretty(shared), title="Initial Shared", border_style="cyan")
        
        self.console.print(Columns([state_view, shared_view]))
        self._pause()

    async def on_step_start(self, state: T, shared: S, nodes: list[NextNode[T, S]]):
        # Tree-Ansicht für die anstehenden Nodes
        node_tree = Tree("[bold yellow]Next Step Nodes")
        for node in nodes:
            node_tree.add(f"[green]{node.node.__class__.__name__}[/green]")

        self.console.print(Panel(
            node_tree,
            title="Step Start",
            border_style="yellow",
            expand=False
        ))
        self._pause()

    async def on_merge_conflict(self, state: T, result_states: list[T], changes: list[dict[tuple[Hashable, ...], Change]], conflicts: dict[tuple[Hashable, ...], list[Change]]):
        self.console.print(Panel(
            f"[bold white]Conflict detected in {len(conflicts)} property path(s)![/]",
            title="ERROR: MERGE CONFLICT",
            style="on red",
            expand=True
        ))

        for path, change_list in conflicts.items():
            conflict_table = Table(title=f"Conflict at: [bold yellow]{path}[/]", show_lines=True)
            conflict_table.add_column("Branch", justify="center", style="dim")
            conflict_table.add_column("Type")
            conflict_table.add_column("Proposed Value", ratio=1)

            for i, change in enumerate(change_list):
                conflict_table.add_row(
                    f"#{i}",
                    str(change.type),
                    Pretty(change.new)
                )
            
            self.console.print(conflict_table)
        
        self.console.print("[bold red]Note:[/bold red] The graph cannot merge these branches automatically.")
        self._pause()

    async def on_merge_end(self, state: T, result_states: list[T], changes: list[dict[tuple[Hashable, ...], Change]], merged_state: T):
        self.console.print(Rule("[bold cyan]Merge Result", style="cyan"))

        if not any(changes):
            self.console.print("[dim italic]No changes detected.[/dim italic]")
        else:
            table = Table(show_lines=True, expand=True)
            table.add_column("Property Path", style="bold yellow")
            table.add_column("Type", justify="center")
            table.add_column("Change", ratio=1)

            for idx, change_dict in enumerate(changes):
                for path, change in change_dict.items():
                    # Farbe basierend auf ChangeType
                    color = "green" if change.type == ChangeTypes.ADDED else "red" if change.type == ChangeTypes.REMOVED else "blue"
                    
                    # Schöne Darstellung des Diffs
                    diff_view = Tree(f"[bold {color}]{change.type.upper()}[/bold {color}]")
                    if change.type != ChangeTypes.ADDED:
                        diff_view.add(Panel(Pretty(change.old), title="old", border_style="red", expand=False))
                    if change.type != ChangeTypes.REMOVED:
                        diff_view.add(Panel(Pretty(change.new), title="new", border_style="green", expand=False))

                    table.add_row(
                        f"{path}\n[dim]Branch {idx}[/dim]",
                        f"[{color}]{change.type}[/]",
                        diff_view
                    )
            
            self.console.print(table)

        # Kleiner, cleaner Footer für den Shared State Vergleich
        self._pause()

    
    async def on_step_end(self, state: T, shared: S, nodes: list[NextNode[T, S]]):
        # Header für den abgeschlossenen Schritt
        node_names = ", ".join([n.node.__class__.__name__ for n in nodes])
        self.console.print(Rule(f"[bold blue]Step Completed: {node_names}", style="blue"))

        # Erstellung einer Vergleichstabelle für State und Shared
        comparison_table = Table(
            title="Post-Step Snapshot",
            show_header=True, 
            header_style="bold cyan",
            expand=True,
            border_style="dim"
        )
        
        comparison_table.add_column("Category", style="bold", width=12)
        comparison_table.add_column("Content", justify="left")

        # Zeile für den lokalen State
        comparison_table.add_row(
            "STATE", 
            Panel(Pretty(state), border_style="green", title="State")
        )
        
        # Zeile für den Shared State
        comparison_table.add_row(
            "SHARED", 
            Panel(Pretty(shared), border_style="yellow", title="Shared State")
        )

        self.console.print(comparison_table)
        
        # Optional: Kleiner Indikator, welche Nodes gerade fertig wurden
        self.console.print(f"[dim]Finished executing: {node_names}[/dim]")
        
        self._pause()

    async def on_graph_end(self, state: T, shared: S):
        self.console.print(Rule("[bold green]Graph Execution Finished", style="green"))
        self.console.print(Panel(Pretty(state), title="Final State", border_style="green"))
        self._pause()