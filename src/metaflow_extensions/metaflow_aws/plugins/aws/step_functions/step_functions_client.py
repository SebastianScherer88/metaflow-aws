from enum import StrEnum
from typing import Iterator

from metaflow.metaflow_config import (
    SFN_EXECUTION_LOG_GROUP_ARN,
)


class CustomStepFunctionsState(StrEnum):
    running = "RUNNING"
    succeeded = "SUCCEEDED"
    failed = "FAILED"
    timed_out = "TIMED_OUT"
    aborted = "ABORTED"


class CustomStepFunctionTags(StrEnum):
    resource_owner_key = "owner"
    resoure_owner_value = "metaflow"

    flow_user_key = "metaflow/user"
    flow_user_value = "SFN"

    flow_name_key = "metaflow/flow_name"

    flow_owner_key = "metaflow/owner"

    flow_branch_key = "metaflow/branch_name"

    flow_project_key = "metaflow/project_name"

    flow_project_name_key = "metaflow/project_flow_name"

    flow_deployment_timestamp_key = "metaflow/deployment_timestamp"

    flow_parameters_key = "metaflow/parameters"


# def aws_resource_tags(
#         flow_name: str,
#         # flow_owner: str,
#         # flow_branch: str = "",
#         # flow_project: str = ""
#     ) -> list[dict[str,str]]:
#     """Generates AWS resource tags containing metaflow metadata to help identify
#     and retrieve resources associated with (Deployed)Flow(s)."""

#     return [
#         {'key':str(CustomStepFunctionTags.resource_owner_key),'value':str(CustomStepFunctionTags.resoure_owner_value)},
#         {'key':str(CustomStepFunctionTags.flow_name_key),'value':flow_name},
#         # {'key':CustomStepFunctionTags.flow_owner_key,'value':flow_owner},
#         # {'key':CustomStepFunctionTags.flow_branch_key,'value':flow_branch},
#         # {'key':CustomStepFunctionTags.flow_project_key,'value':flow_project},
#     ]


class CustomStepFunctionsClient(object):
    def __init__(self):
        from metaflow.plugins.aws.aws_client import get_aws_client

        self._client = get_aws_client("stepfunctions")
        self._tagging_client = get_aws_client("resourcegoupstaggingapi")

    # def search(self, name: str):
    #     paginator = self._client.get_paginator("list_state_machines")
    #     return next(
    #         (
    #             state_machine
    #             for page in paginator.paginate()
    #             for state_machine in page["stateMachines"]
    #             if state_machine["name"] == name
    #         ),
    #         None,
    #     )

    def create(
        self,
        flow_name: str,
        definition: str,
        role_arn: str,
        log_execution_history: bool,
        tags: list[dict[str, str]],
    ) -> str:
        """Creates a state machine as a metaflow Flow deployment."""
        try:
            response = self._client.create_state_machine(
                name=flow_name,
                definition=definition,
                roleArn=role_arn,
                loggingConfiguration=self._default_logging_configuration(
                    log_execution_history
                ),
                tags=tags,
                # tags=aws_resource_tags(flow_name=flow_name)
            )
            state_machine_arn = response["stateMachineArn"]
        except self._client.exceptions.StateMachineAlreadyExists as e:
            # State Machine already exists, update it instead of creating it.
            state_machine_arn = e.response["Error"]["Message"].split("'")[1]
            self._client.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=definition,
                roleArn=role_arn,
                loggingConfiguration=self._default_logging_configuration(
                    log_execution_history
                ),
                tags=tags,
                # tags=aws_resource_tags(flow_name=flow_name)
            )
        return state_machine_arn

    def get_tags(self, state_machine_arn: str) -> dict[str, str] | None:
        """Get the tags from the specified state machine.

        Args:
            state_machine_arn (str): The ARN of the state machine.

        Returns:
            dict[str,str]: The tags.
        """
        try:
            response = self._client.list_tags_for_resource(
                resourceArn=state_machine_arn
            )
            return {tag["key"]: tag["value"] for tag in response["tags"]}
        except self._client.exceptions.StateMachineDoesNotExist:
            return None

    # def get(self, name: str):
    #     state_machine_arn = self.get_state_machine_arn(name)
    #     if state_machine_arn is None:
    #         return None
    #     try:
    #         return self._client.describe_state_machine(
    #             stateMachineArn=state_machine_arn,
    #         )
    #     except self._client.exceptions.StateMachineDoesNotExist:
    #         return None

    def create_execution(self, state_machine_arn: str, input: str) -> str:
        """
        Creates a state machine execution as a metaflow Run by invoking the
        specified state machine with the specified inputs.
        """

        response = self._client.start_execution(
            stateMachineArn=state_machine_arn, input=input
        )

        return response["executionArn"]

    def list_arns(self, flow_name: str | None = None) -> Iterator[str]:
        """Lists the arns the state machines managed by metaflow and inside
        the optional name scope."""

        # build filter tags
        tag_filters = [
            {
                "Key": str(CustomStepFunctionTags.resource_owner_key),
                "Values": [str(CustomStepFunctionTags.resoure_owner_value)],
            }
        ]

        if flow_name is not None:
            tag_filters.append(
                {
                    "Key": str(CustomStepFunctionTags.flow_name_key),
                    "Values": [flow_name],
                }
            )

        # retrieve the arns of all state amchines that are
        # - managed by metaflow
        # - (optional) carry the specified flow name
        paginator = self._tagging_client.get_paginator("get_resources")

        for page in paginator.paginate(
            ResourceTypeFilters=["states:stateMachine"],
            TagFilters=tag_filters,
        ):
            for resource in page["ResourceTagMappingList"]:
                yield resource["ResourceARN"]

    def list_executions(
        self, state_machine_arn: str, states: list[CustomStepFunctionsState]
    ) -> Iterator[dict[str, str]]:
        """
        Retrieves relevant state machine execution responses for the specified
        state machine in the form of dictionaries of the following format:
        {
            'executionArn': 'string',
            'stateMachineArn': 'string',
            'name': 'string',
            'status': 'RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED'|'PENDING_REDRIVE',
            'startDate': datetime(2015, 1, 1),
            'stopDate': datetime(2015, 1, 1),
            'mapRunArn': 'string',
            'itemCount': 123,
            'stateMachineVersionArn': 'string',
            'stateMachineAliasArn': 'string',
            'redriveCount': 123,
            'redriveDate': datetime(2015, 1, 1)
        }
        """
        if len(states) > 0:
            return (
                execution
                for state in states
                for page in self._client.get_paginator("list_executions").paginate(
                    stateMachineArn=state_machine_arn, statusFilter=str(state)
                )
                for execution in page["executions"]
            )
        return (
            execution
            for page in self._client.get_paginator("list_executions").paginate(
                stateMachineArn=state_machine_arn
            )
            for execution in page["executions"]
        )

    def terminate_execution(self, execution_arn: str) -> dict[str, str]:
        """Terminates the specified state machine execution."""
        try:
            response = self._client.stop_execution(executionArn=execution_arn)
            return response
        except self._client.exceptions.ExecutionDoesNotExist:
            raise ValueError("The execution ARN %s does not exist." % execution_arn)
        except Exception as e:
            raise e

    def _default_logging_configuration(self, log_execution_history: bool):
        if log_execution_history:
            return {
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": SFN_EXECUTION_LOG_GROUP_ARN
                        }
                    }
                ],
            }
        else:
            return {"level": "OFF"}

    # def get_state_machine_arn(self, name: str):
    #     if AWS_SANDBOX_ENABLED:
    #         # We can't execute list_state_machines within the sandbox,
    #         # but we can construct the statemachine arn since we have
    #         # explicit access to the region.
    #         from metaflow.plugins.aws.aws_client import get_aws_client

    #         account_id = get_aws_client("sts").get_caller_identity().get("Account")
    #         region = AWS_SANDBOX_REGION
    #         # Sandboxes are in aws partition
    #         return "arn:aws:states:%s:%s:stateMachine:%s" % (region, account_id, name)
    #     else:
    #         state_machine = self.search(name)
    #         if state_machine:
    #             return state_machine["stateMachineArn"]
    #         return None

    def delete(self, flow_name: str) -> dict | None:
        """Deletes the associated metaflow flow's state machine and schedule"""

        state_machine_arns = list(self.list_arns(flow_name=flow_name))
        if state_machine_arns == []:
            return None
        return self._client.delete_state_machine(
            stateMachineArn=state_machine_arns[0],
        )
