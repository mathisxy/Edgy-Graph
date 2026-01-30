from edgygraph import State, Shared, Node, START, END, Graph
from random import randint
import asyncio

### STATE

class GamblingState(State):

    user_guess: int | None = None
    number: int | None = None
    try_count: int = 0
    

### NODES

class GuessNode(Node[GamblingState, Shared]):

    async def run(self, state: GamblingState, shared: Shared) -> None:
        
        guess_str = input(f"Guess a number between 1 and 10: ")

        try:
            state.user_guess = int(guess_str)
            state.try_count += 1
        except ValueError:
            print("Invalid input. Please enter a number.")
            state.user_guess = None


class RollDiceNode(Node[GamblingState, Shared]):

    async def run(self, state: GamblingState, shared: Shared) -> None:
        
        assert state.user_guess is not None

        state.number = randint(1, 10)


class FailNode(Node[GamblingState, Shared]):

    async def run(self, state: GamblingState, shared: Shared) -> None:
        
        assert state.user_guess is not None
        assert state.number is not None

        print(f"The number is {state.number}. You guessed {state.user_guess}. Try again!")
        state.user_guess = None
        state.number = None


class WinNode(Node[GamblingState, Shared]):

    async def run(self, state: GamblingState, shared: Shared) -> None:

        assert state.user_guess is not None
        assert state.number is not None

        if state.try_count == 1:
            print(f"That's amazing! The number is {state.number}. You won with your first try!")

        print(f"The number is {state.number}. You won with {state.try_count} tries!")


### INSTANCES

state = GamblingState()
shared = Shared()

guess = GuessNode()
roll = RollDiceNode()
fail = FailNode()
win = WinNode()


### GRAPH

asyncio.run(Graph[GamblingState, Shared](
    edges=[
        (
            START,
            guess
        ),
        (
            guess,
            lambda st, sh: roll if st.user_guess is not None else guess
        ),
        (
            roll,
            lambda st, sh: win if st.user_guess == st.number else fail
        ),
        (
            fail,
            guess
        ),
        (
            win,
            END
        )
    ]
)(state, shared))