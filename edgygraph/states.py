from pydantic import BaseModel

class GraphState(BaseModel):
    vars: dict[str, object] = {}