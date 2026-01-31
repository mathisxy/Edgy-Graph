# Typed Graph-based Pipeline Builder

[![Pipy](https://img.shields.io/pypi/v/edgygraph)](https://pypi.org/project/edgygraph/)
[![Downloads](https://img.shields.io/pypi/dm/edgygraph)](https://pypi.org/project/edgygraph/#files)
[![Issues](https://img.shields.io/github/issues/mathisxy/edgygraph)](https://github.com/mathisxy/edgygraph/issues)
[![Deploy Docs](https://github.com/mathisxy/edgygraph/actions/workflows/docs.yml/badge.svg)](https://github.com/mathisxy/Edgy-Graph/actions/workflows/docs.yml)
[![Documentation](https://img.shields.io/badge/Docs-GitHub%20Pages-blue)](https://mathisxy.github.io/edgygraph/)

A **pydantically** typed, lightweight **graph framework** for Python that combines features from [Langgraph](https://github.com/langchain-ai/langgraph) with **static type security**.

## Overview

Edgy Graph is a framework for building and executing graph-based pipelines. It supports:

- **Pydantic Typing** Full type safety with Pydantic and Generics
- **Inheritance and Variance**: Extendable state and node classes
- **Parallel Task Processing**: Multiple nodes can execute simultaneously
- **State Management**: Best of both worlds; state management with conflict detection and shared instance with lock
- **Flexible Routing**: Simple or complex; dynamic path decisions based on functions or simple node to node edges
- **Streaming**: Standardized interface for streaming to next nodes

## Installation

### PyPI
```bash
pip install edgygraph
```
> Python 3.13 or higher is required


## Example Workflow

### Import Classes

```python
from edgygraph import State, Shared, Node, START, END, Graph
import asyncio
```

### Create a State

```python
class MyState(State):

    capslock: bool = False
```

### Create a Node

```python
class MyNode(Node[MyState, Shared]):

    async def run(self, state: MyState, shared: Shared) -> None:

        if state.capslock:
            print("HELLO WORLD!")
        else:
            print("Hello World!")
```

### Create Instances

```python
state = MyState(capslock=True)
shared = Shared()

node = MyNode()
```

### Create a Graph

```python
Graph[MyState, Shared](
    edges=[
        (
            START,
            node
        ),
        (
            node,
            END
        )
    ]
)
```

### Run Graph

```python
asyncio.run(graph(state, shared))
```
