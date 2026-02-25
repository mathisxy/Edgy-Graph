from collections.abc import Hashable

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.pretty import Pretty
from rich.rule import Rule
from rich.tree import Tree

from ...diff import Change, ChangeTypes
from ...graph.types import NextNode
from ...states import StateProtocol as State, SharedProtocol as Shared


class GraphRenderer[T: State, S: Shared]:
    """
    A reusable Rich-based renderer for graph execution state.

    Provides methods to render state snapshots, step info, merge results,
    and other graph lifecycle events. Can be used by hooks like
    InteractiveDebugHook or LoggingHook without duplicating display logic.
    """

    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()

    # -------------------------------------------------------------------------
    # Graph lifecycle
    # -------------------------------------------------------------------------

    def render_graph_start(self, state: T, shared: S) -> None:
        self.console.print(Rule("[bold magenta]Graph Execution Started", style="magenta"))
        self.console.print(Columns([
            Panel(Pretty(state), title="Initial State", border_style="blue"),
            Panel(Pretty(shared), title="Initial Shared", border_style="cyan"),
        ]))

    def render_graph_end(self, state: T, shared: S) -> None:
        self.console.print(Rule("[bold green]Graph Execution Finished", style="green"))
        # self.console.print(Panel(Pretty(state), title="Final State", border_style="green"))
        self.console.print(Columns([
            Panel(Pretty(state), title="Final State", border_style="blue"),
            Panel(Pretty(shared), title="Final Shared", border_style="cyan"),
        ]))

    # -------------------------------------------------------------------------
    # Step lifecycle
    # -------------------------------------------------------------------------

    def render_step_start(self, nodes: list[NextNode[T, S]]) -> None:
        node_tree = Tree("[bold yellow]Next Step Nodes")
        for node in nodes:
            node_tree.add(f"[green]{node.node.__class__.__name__}[/green]")

        self.console.print(Panel(
            node_tree,
            title="Step Start",
            border_style="yellow",
            expand=False,
        ))

    def render_step_end_rule(self, nodes: list[NextNode[T, S]]) -> None:
        node_names = self._node_names(nodes)
        self.console.print(Rule(f"[bold blue]Step Completed: {node_names}", style="blue"))

    def render_step_end_footer(self, nodes: list[NextNode[T, S]]) -> None:
        node_names = self._node_names(nodes)
        self.console.print(f"[dim]Finished executing: {node_names}[/dim]")


    def render_step_end(self, state: T, shared: S, nodes: list[NextNode[T, S]]) -> None:
        self.render_step_end_rule(nodes)

        table = Table(
            title="Post-Step Snapshot",
            show_header=True,
            header_style="bold cyan",
            expand=True,
            border_style="dim",
        )
        table.add_column("Category", style="bold", width=12)
        table.add_column("Content", justify="left")
        table.add_row("STATE", Panel(Pretty(state), border_style="green", title="State"))
        table.add_row("SHARED", Panel(Pretty(shared), border_style="yellow", title="Shared State"))

        self.console.print(table)
        self.render_step_end_footer(nodes)

    # -------------------------------------------------------------------------
    # Merge lifecycle
    # -------------------------------------------------------------------------

    def render_merge_conflict(
        self,
        conflicts: dict[tuple[Hashable, ...], list[Change]],
    ) -> None:
        self.console.print(Panel(
            f"[bold white]Conflict detected in {len(conflicts)} property path(s)![/]",
            title="ERROR: MERGE CONFLICT",
            style="on red",
            expand=True,
        ))

        for path, change_list in conflicts.items():
            table = Table(title=f"Conflict at: [bold yellow]{path}[/]", show_lines=True)
            table.add_column("Branch", justify="center", style="dim")
            table.add_column("Type")
            table.add_column("Proposed Value", ratio=1)

            for i, change in enumerate(change_list):
                table.add_row(f"#{i}", str(change.type), Pretty(change.new))

            self.console.print(table)

        self.console.print("[bold red]Note:[/bold red] The graph cannot merge these branches automatically.")

    def render_merge_end(
        self,
        changes: list[dict[tuple[Hashable, ...], Change]],
    ) -> None:
        self.console.print(Rule("[bold cyan]Merge Result", style="cyan"))

        if not any(changes):
            self.console.print("[dim italic]No changes detected.[/dim italic]")
            return

        table = Table(show_lines=True, expand=True)
        table.add_column("Property Path", style="bold yellow")
        table.add_column("Type", justify="center")
        table.add_column("Change", ratio=1)

        for idx, change_dict in enumerate(changes):
            for path, change in change_dict.items():
                color = (
                    "green" if change.type == ChangeTypes.ADDED
                    else "red" if change.type == ChangeTypes.REMOVED
                    else "blue"
                )

                diff_view = Tree(f"[bold {color}]{change.type.upper()}[/bold {color}]")
                if change.type != ChangeTypes.ADDED:
                    diff_view.add(Panel(Pretty(change.old), title="old", border_style="red", expand=False))
                if change.type != ChangeTypes.REMOVED:
                    diff_view.add(Panel(Pretty(change.new), title="new", border_style="green", expand=False))

                table.add_row(
                    f"{path}\n[dim]Branch {idx}[/dim]",
                    f"[{color}]{change.type}[/]",
                    diff_view,
                )

        self.console.print(table)

    # -------------------------------------------------------------------------
    # Generic helpers
    # -------------------------------------------------------------------------

    def render_rule(self, title: str = "", style: str = "dim") -> None:
        self.console.print(Rule(title, style=style))

    def _node_names(self, nodes: list[NextNode[T, S]]) -> str:
        return ", ".join(n.node.__class__.__name__ for n in nodes)