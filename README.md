# Typed Graph-based Pipeline Builder

[![Pipy](https://img.shields.io/pypi/v/edgygraph)](https://pypi.org/project/edgygraph/)
[![Downloads](https://img.shields.io/pypi/dm/edgygraph)](https://pypi.org/project/edgygraph/#files)
[![Issues](https://img.shields.io/github/issues/mathisxy/edgygraph)](https://github.com/mathisxy/edgygraph/issues)
[![Deploy Docs](https://github.com/mathisxy/edgygraph/actions/workflows/docs.yml/badge.svg)](https://github.com/mathisxy/Edgy-Graph/actions/workflows/docs.yml)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://mathisxy.github.io/edgygraph/)

> **Status**: 🚧 In Development

A **pydantically** typed, lightweight **graph framework** for Python that combines features from [Langgraph](https://github.com/langchain-ai/langgraph) with **static type security**.

## Overview

Edgy Graph is a framework for building and executing graph-based pipelines. It supports:

- **Pydantic Typing** Full type safety with Pydantic and Generics
- **Inheritance and Variance**: Expandable state and node classes with inheritance
- **Asynchronous Execution**: Full `async/await` support for nodes
- **Parallel Task Processing**: Multiple nodes can execute simultaneously
- **State Management**: Best of both worlds; state management with conflict detection and shared instance with lock
- **Flexible Routing**: Simple or complex; dynamic path decisions based on functions or simple node to node edges
- **Streaming**: Standardized interface for streaming to next nodes

## Installation

### PyPI
```
pip install edgygraph
```
> Python 3.13 or higher is required
