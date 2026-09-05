import datetime

from metaflow import FlowSpec, project, schedule, step
from metaflow.parameters import Parameter


# token: basicflow-0-sudm
@schedule(cron="0 * * * ? *")
@project(name="test_project")
class ScheduledFlow(FlowSpec):
    greeting_name = Parameter(
        "greeting_name", help="Who to greet.", type=str, default="Sebastian"
    )
    fruit_list = Parameter(
        "fruit_list",
        help="List of things to snack on.",
        type=str,
        default="apple,banana,coconut",
    )

    @step
    def start(self):
        # 👇 This becomes an artifact
        self.message = f"Hello {self.greeting_name}! It is {datetime.datetime.now()}. From metaflow :)"

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
        self.next(self.end)

    # conditional
    @step
    def end(self):
        print("Done greeting and snacking.")


if __name__ == "__main__":
    ScheduledFlow()
