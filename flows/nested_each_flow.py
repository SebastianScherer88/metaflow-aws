from metaflow import Config, FlowSpec, step
from metaflow.parameters import Parameter


class ForEachFlow(FlowSpec):
    config = Config("config", default="../pyproject.toml", parser="tomllib.loads")

    alpha = Parameter(name="param-1", default=1, help="Testing purposes")
    beta = Parameter(name="param-2", default="TEST", help="Also for tests")
    gamma = Parameter(
        name="param-3", help="Test if versions of state machiens get created"
    )

    @step
    def start(self):
        self.next(self.root)

    @step
    def root(self):
        self.split_1 = ["a", "b", "c"]
        self.next(self.nest_1, foreach="split_1")

    @step
    def nest_1(self):
        self.split_2 = ["d", "e", "f", "g"]
        self.next(self.nest_2, foreach="split_2")

    @step
    def nest_2(self):
        foo = self.foreach_stack()
        print(foo)
        self.next(self.join_2)

    @step
    def join_2(self, inputs):
        for inp in inputs:
            print(inp)
        self.next(self.join_1)

    @step
    def join_1(self, inputs):
        for inp in inputs:
            print(inp)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForEachFlow()
