# Typed Graph-based Pipeline Builder

![Pipy](https://img.shields.io/pypi/v/edgygraph)
![Downloads](https://img.shields.io/pypi/dm/edgygraph)
![Issues](https://img.shields.io/github/issues/mathisxy/Edgy-Graph)

> **Status**: 🚧 In Development

A **pydantically** typed, lightweight **graph framework** for Python that combines features from [Langgraph](https://github.com/langchain-ai/langgraph) with **static type security**.

## Overview

Edgy Graph is a framework for building and executing graph-based pipelines. It supports:

- **Asynchronous Execution**: Full `async/await` support for nodes
- **Parallel Task Processing**: Multiple nodes can execute simultaneously
- **State Management**: Safe state management with conflict detection
- **Generic Typing**: Fully typed with Python Generics
- **Inheritance and Variance**: Expand state and node classes with multi-inheritance
- **Flexible Routing**: Dynamic path decisions based on functions
- **Streaming**: Standardized interface for streaming from node to node

## Installation

### PyPI
```
pip install edgygraph
```
> Python 3.13 or higher is required
