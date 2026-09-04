import json
from enum import StrEnum
from typing import Any, Iterator

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


class CustomStepFunctionsClient(object):
    def __init__(self):
        from metaflow.plugins.aws.aws_client import get_aws_client

        self._client = get_aws_client("stepfunctions")
        self._tagging_client = get_aws_client("resourcegroupstaggingapi")

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
            )
            self._client.tag_resource(resourceArn=state_machine_arn, tags=tags)
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

    def get_environment_override(
        self, state_machine_arn: str, environment_variable_name: str
    ) -> list[dict[str, str]]:
        """Retrieves the value of the specified environment variable from the
        metaflow flows state machine definition, specifically the StartAt
        step's Parameters.ContainerOverrides.Environment seciont.

        Args:
            state_machine_arn (str): The ARN of the state machine.
            environment_variable_name (str): The name of the environment
                variable to extract the spec for.

        Returns:
            list[dict[str,str]]: The environment variable spec dict with keys
                'Name' and 'Value', if the environment variable was present in
                the override section. If not, returns an empty list.
        """

        state_machine_description: dict = self._client.describe_state_machine(
            stateMachineArn=state_machine_arn,
        )
        definition = json.loads(state_machine_description["definition"])
        start_state_name = definition.get("StartAt", "start")
        start_state = definition["States"][start_state_name]
        start_state_environment = start_state["Parameters"]["ContainerOverrides"][
            "Environment"
        ]
        environment_variable_spec = [
            env_spec
            for env_spec in start_state_environment
            if env_spec["Name"] == environment_variable_name
        ]

        return environment_variable_spec

    def get_production_token(self, state_machine_arn: str) -> str | None:
        """Retrieves the production token  from the deployed flow's state
        machine definition.

        Args:
            state_machine_arn (str): The ARN of the state machine.

        Returns:
            str | None: The production token, if present in the definition.
                Otherwise returns None.
        """

        metaflow_produciton_token_spec = self.get_environment_override(
            state_machine_arn=state_machine_arn,
            environment_variable_name="METAFLOW_PRODUCTION_TOKEN",
        )

        if metaflow_produciton_token_spec:
            token = metaflow_produciton_token_spec[0]["value"]
        else:
            token = None

        return token

    def get_all_flow_parameters(
        self, state_machine_arn: str
    ) -> dict[str, dict[str, Any]] | None:
        """Retrieves the metaflow parameter context required to reconstruct
        a tempfile for the deployed flow as required by the from_deployment
        mechanism.

        Args:
            state_machine_arn (str): The ARN of the state machine.

        Returns:
            dict[str,str] | None: The dictionary containing all the required
                parameters meta data to re-construct a DeployedFlow object.
        """

        metaflow_all_parameters_spec = self.get_environment_override(
            state_machine_arn=state_machine_arn,
            environment_variable_name="METAFLOW_ALL_PARAMETERS",
        )

        if metaflow_all_parameters_spec:
            parameters = json.loads(metaflow_all_parameters_spec[0]["Value"])
        else:
            parameters = {}

        return parameters

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
        """Lists the arns of the state machines managed by metaflow and inside
        the optional name scope.

        Args:
            flow_name (str | None, optional): The name of the state machine.
                Defaults to None.

        Yields:
            Iterator[str]: The ARNs of the state machines inside the search
                scope.
        """

        # build filter tags
        tag_filters = [
            {
                "Key": str(CustomStepFunctionTags.resource_owner_key),
                "Values": [str(CustomStepFunctionTags.resoure_owner_value)],
            }
        ]

        # retrieve the arns and names of all state machines that are managed
        # by metaflow
        paginator = self._tagging_client.get_paginator("get_resources")
        matched_state_machines: list[tuple[str, str]] = []

        for page in paginator.paginate(
            ResourceTypeFilters=["states:stateMachine"],
            TagFilters=tag_filters,
        ):
            for resource in page["ResourceTapMappingList"]:
                resource_arn = resource["ResourceARN"]
                resource_name = resource_arn.split(":")[-1]
                matched_state_machines.append((resource_arn, resource_name))

        # if a name is provided, filter down to the state machines matching it
        if flow_name is not None:
            matched_state_machines = [
                msm for msm in matched_state_machines if msm[1] == flow_name
            ]

        for matched_state_machine in matched_state_machines:
            yield matched_state_machine[0]

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

    def delete(self, flow_name: str) -> dict | None:
        """Deletes the associated metaflow flow's state machine and schedule"""

        state_machine_arns = list(self.list_arns(flow_name=flow_name))
        if state_machine_arns == []:
            return None
        return self._client.delete_state_machine(
            stateMachineArn=state_machine_arns[0],
        )
