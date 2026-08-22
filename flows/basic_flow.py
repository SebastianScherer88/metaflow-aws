from metaflow import FlowSpec, step, batch
# token: basicflow-0-sudm
class BasicFlow(FlowSpec):

    @step
    def start(self):
        print("hello from metaflow")
        
        # 👇 This becomes an artifact
        self.message = "hello artifact world"

        self.next(self.end)
        
    @step
    def end(self):
        print("done")

if __name__ == "__main__":
    BasicFlow()