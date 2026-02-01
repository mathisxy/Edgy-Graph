# Typed Graph-based Pipeline Builder

[![Pipy](https://img.shields.io/pypi/v/edgygraph)](https://pypi.org/project/edgygraph/)
[![Downloads](https://img.shields.io/pypi/dm/edgygraph)](https://pypi.org/project/edgygraph/#files)
[![Issues](https://img.shields.io/github/issues/mathisxy/edgygraph)](https://github.com/mathisxy/edgygraph/issues)
[![Type Check](https://github.com/mathisxy/edgygraph/actions/workflows/typecheck.yml/badge.svg?branch=main)](https://github.com/mathisxy/Edgy-Graph/actions/workflows/typecheck.yml)
[![Deploy Docs](https://github.com/mathisxy/edgygraph/actions/workflows/docs.yml/badge.svg)](https://github.com/mathisxy/Edgy-Graph/actions/workflows/docs.yml)
[![Documentation](https://img.shields.io/badge/Docs-GitHub%20Pages-blue)](https://mathisxy.github.io/edgygraph/)

A **pydantically** typed, lightweight **graph framework** for Python that combines features from [Langgraph](https://github.com/langchain-ai/langgraph) with **static type security**.

A community collection of nodes will be available ![here](https://www.github.com/mathisxy/edgynodes/)

## Overview

Edgy Graph is a framework for building and executing graph-based pipelines. It supports:

- **Pydantic Typing** <br> Built on Pydantic and Generics for complete static type safety.
- **Inheritance and Variance**: <br> Easily extend and specialize state and node classes.
- **Parallel Task Processing**: <br> Multiple nodes can run simultaneously
- **Dual State Management**:
    - State with automatic change extraction and conflict detection
    - Shared state accessible by all nodes, protected via explicit locking
- **Flexible Routing**: <br> Define simple node-to-node edges or dynamic routing based on functions.
- **Streaming**: <br> A standardized interface for streaming data between nodes.

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

<br>

 > More examples can be found in the examples folder


