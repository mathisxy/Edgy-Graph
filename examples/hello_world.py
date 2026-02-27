from edgygraph import State, Shared, Node, START, END, Graph
import asyncio


### STATES

class MyState(State):

    capslock: bool = False


### NODES

class MyNode(Node[MyState, Shared]):

    async def __call__(self, state: MyState, shared: Shared) -> None:

        if state.capslock:
            print("HELLO WORLD!")
        else:
            print("Hello World!")


### INSTANCES

state = MyState(capslock=True)
shared = Shared()

node = MyNode()


### GRAPH

graph = Graph[MyState, Shared](
    edges=[(
        (
            START,
            node
        ),
        END)
    ]
)


### RUN

asyncio.run(graph(state, shared))