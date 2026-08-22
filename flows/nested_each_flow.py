from metaflow import FlowSpec, step


class ForEachFlow(FlowSpec):
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