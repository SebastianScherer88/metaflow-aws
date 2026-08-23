import random

from metaflow import FlowSpec, step
from metaflow.parameters import Parameter


# token: basicflow-0-sudm
class BasicFlow(FlowSpec):
    greeting_name = Parameter(
        "greeting_name", help="Who to greet.", type=str, default="Sebastian"
    )
    fruit_list = Parameter(
        "fruit_list",
        help="List of things to snack on.",
        type=str,
        default="apple,banana,coconut",
    )
    n_loops = Parameter(
        "n_loops", help="Number of recursions for loop step", type=int, default=3
    )

    @step
    def start(self):
        # 👇 This becomes an artifact
        self.message = f"Hello {self.greeting_name}! From metaflow :)"

        print(self.message)

        self.next(self.snack_prep)

    # foreach
    @step
    def snack_prep(self):
        self.fruits_parsed = self.fruit_list.split(",")
        self.next(self.snack_time, foreach="fruits_parsed")

    @step
    def snack_time(self):
        self.snack_suggestion = f"Hungry? Here, have one {self.input}"
        print(self.snack_suggestion)
        self.next(self.snack_cleanup)

    @step
    def snack_cleanup(self, inputs):
        self.snack_suggestions = [input.snack_suggestion for input in inputs]
        self.next(self.flip_coin)

    # conditional
    @step
    def flip_coin(self):
        print("Flipping a coin...")
        self.flipped_coin = random.choice(["Heads", "Tails"])

        self.next({"Heads": self.heads, "Tails": self.tails}, condition="flipped_coin")

    @step
    def heads(self):
        print("Coin shows Heads!")
        self.next(self.loop)

    @step
    def tails(self):
        print("Coin shows Tails!")
        self.next(self.loop)

    # recursion
    @step
    def loop(self):
        self.counter = getattr(self, "counter", 0) + 1
        print("Loop counter is", self.counter)
        self.again = self.counter <= self.n_loops
        self.next({True: self.loop, False: self.end}, condition="again")

    @step
    def end(self):
        print("Done greeting, snacking and looping.")


if __name__ == "__main__":
    BasicFlow()
