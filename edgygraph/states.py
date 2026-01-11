from pydantic import BaseModel
from llm_ir import AIMessage

class GraphState(BaseModel):
    messages: list[AIMessage] = []
    vars: dict[str, object] = {}