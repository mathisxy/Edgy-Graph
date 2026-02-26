from ..graph.hooks import GraphHook
from ..states import StateProtocol, SharedProtocol
from ..graph.types import NextNode
from .utils.rich_printing import GraphRenderer

class NodePrintHook[T: StateProtocol = StateProtocol, S: SharedProtocol = SharedProtocol](GraphHook[T, S]):
    """
    A hook that prints the execiting nodes before and after execution in each branch.

    Args:
        renderer: The GraphRenderer to use for printing the nodes. If not provided, the default renderer will be used.
    """

    def __init__(self, renderer: GraphRenderer[T, S] | None = None) -> None:
        self.renderer = renderer or GraphRenderer()


    async def on_graph_start(self, state: T, shared: S) -> None:
        self.renderer.render_graph_start(state, shared)

    async def on_step_start(self, state: T, shared: S, nodes: list[NextNode[T, S]]) -> None:
        self.renderer.render_step_start(nodes)

    async def on_step_end(self, state: T, shared: S, nodes: list[NextNode[T, S]]) -> None:
        self.renderer.render_step_end_footer(nodes)
        
    async def on_graph_end(self, state: T, shared: S) -> None:
        self.renderer.render_graph_end(state, shared)