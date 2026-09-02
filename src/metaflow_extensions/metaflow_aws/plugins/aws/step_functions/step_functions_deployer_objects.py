import json
import sys
import tempfile
from typing import ClassVar, Iterator, Optional

from metaflow.client.core import get_metadata
from metaflow.exception import MetaflowException
from metaflow.runner.deployer import (
    DeployedFlow,
    Deployer,
    TriggeredRun,
    generate_fake_flow_file_contents,
)
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

from .step_functions import CustomStepFunctions
from .step_functions_client import (
    CustomStepFunctionsClient,
    CustomStepFunctionsState,
    CustomStepFunctionTags,
)


class CustomStepFunctionsTriggeredRun(TriggeredRun):
    """
    A class representing a triggered AWS Step Functions state machine execution.
    """

    def terminate(self, **kwargs) -> bool:
        """
        Terminate the running state machine execution.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize the termination with a production token.

        Returns
        -------
        bool
            True if the command was successful, False otherwise.
        """
        _, run_id = self.pathspec.split("/")

        # every subclass needs to have `self.deployer_kwargs`
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).terminate(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        command_obj.sync_wait()
        return command_obj.process.returncode == 0


class CustomStepFunctionsDeployedFlow(DeployedFlow):
    """
    A class representing a deployed AWS Step Functions state machine.
    """

    TYPE: ClassVar[Optional[str]] = "step-functions"

    @classmethod
    def list_deployed_flows(
        cls, flow_name: Optional[str] = None
    ) -> Iterator["CustomStepFunctionsDeployedFlow"]:
        """
        List all deployed AWS Stepfunctions state machines.

        Parameters
        ----------
        flow_name : str, optional, default None
            If specified, only list deployed flows for this specific flow name.
            If None, list all deployed flows.

        Yields
        ------
        CustomStepFunctionsDeployedFlow
            `CustomStepFunctionsDeployedFlow` objects representing deployed
            state machines on AWS Stepfunctions.
        """
        from .step_functions import CustomStepFunctions

        # When flow_name is None, use all=True to get all templates
        # When flow_name is specified, use all=False to filter by flow_name
        for state_machine_arn in CustomStepFunctions.list_arns(flow_name=flow_name):
            try:
                deployed_flow = cls.from_deployment(state_machine_arn)
                yield deployed_flow
            except Exception:
                # Skip templates that can't be converted to DeployedFlow objects
                continue

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None):
        """
        Retrieves a deployed flow based on a state machine arn
        Raises
        ------
        NotImplementedError
            This method is not implemented for Step Functions.
        """
        client = CustomStepFunctionsClient()
        deployment_tags = client.get_tags(identifier)
        parameters = client.get_parameters(identifier)

        if deployment_tags is None:
            raise MetaflowException("No deployed flow found for: %s" % identifier)

        flow_name = deployment_tags.get(CustomStepFunctionTags.flow_name_key, "")
        username = deployment_tags.get(CustomStepFunctionTags.flow_owner_key, "")

        # these two only exist if @project decorator is used..
        branch_name = deployment_tags.get(CustomStepFunctionTags.flow_branch_key, None)
        project_name = deployment_tags.get(
            CustomStepFunctionTags.flow_project_name_key, None
        )

        project_kwargs = {}
        if branch_name is not None:
            if branch_name.startswith("prod."):
                project_kwargs["production"] = True
                project_kwargs["branch"] = branch_name[len("prod.") :]
            elif branch_name.startswith("test."):
                project_kwargs["branch"] = branch_name[len("test.") :]
            elif branch_name == "prod":
                project_kwargs["production"] = True

        fake_flow_file_contents = generate_fake_flow_file_contents(
            flow_name=flow_name, param_info=parameters, project_name=project_name
        )

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as fake_flow_file:
            with open(fake_flow_file.name, "w") as fp:
                fp.write(fake_flow_file_contents)

            if branch_name is not None:
                d = Deployer(
                    fake_flow_file.name,
                    env={"METAFLOW_USER": username},
                    **project_kwargs,
                ).step_functions()
            else:
                d = Deployer(
                    fake_flow_file.name, env={"METAFLOW_USER": username}
                ).step_functions(name=flow_name)

            d.name = identifier.split(":")[-1]
            d.flow_name = flow_name

            if d.name != d.flow_name:
                raise ValueError(
                    f"Resolved flow name {d.flow_name} is not equal to "
                    f"resolved deployment name {d.name}."
                )
            if metadata is None:
                d.metadata = get_metadata()
            else:
                d.metadata = metadata

        return cls(deployer=d)

    @classmethod
    def get_triggered_run(
        cls, identifier: str, run_id: str, metadata: Optional[str] = None
    ):
        """
        This method is not currently implemented for Step Functions.

        Raises
        ------
        NotImplementedError
            This method is not implemented for Step Functions.
        """
        raise NotImplementedError(
            "get_triggered_run is not implemented for StepFunctions"
        )

    @property
    def production_token(self: DeployedFlow) -> Optional[str]:
        """
        Get the production token for the deployed flow.

        Returns
        -------
        str, optional
            The production token, None if it cannot be retrieved.
        """
        try:
            _, production_token = CustomStepFunctions.get_existing_deployment(
                self.deployer.name
            )
            return production_token
        except TypeError:
            return None

    def list_runs(
        self, states: list[CustomStepFunctionsState] | None = None
    ) -> list[CustomStepFunctionsTriggeredRun]:
        """
        List runs of the deployed flow.

        Parameters
        ----------
        states : List[str], optional, default None
            A list of states to filter the runs by. Allowed values are:
            RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED.
            If not provided, all states will be considered.

        Returns
        -------
        List[CustomStepFunctionsTriggeredRun]
            A list of TriggeredRun objects representing the runs of the deployed flow.

        Raises
        ------
        ValueError
            If any of the provided states are invalid or if there are duplicate states.
        """
        VALID_STATES = {
            CustomStepFunctionsState.running,
            CustomStepFunctionsState.aborted,
            CustomStepFunctionsState.timed_out,
            CustomStepFunctionsState.succeeded,
            CustomStepFunctionsState.failed,
        }

        if states is None:
            states = []

        unique_states = set(states)
        if not unique_states.issubset(VALID_STATES):
            invalid_states = unique_states - VALID_STATES
            raise ValueError(
                f"Invalid states found: {invalid_states}. Valid states are: {VALID_STATES}"
            )

        if len(states) != len(unique_states):
            raise ValueError("Duplicate states are not allowed")

        triggered_runs = []
        executions = CustomStepFunctions.list_executions(self.deployer.name, states)

        for execution in executions:
            run_id = "sfn-%s" % execution["name"]
            triggered_run = CustomStepFunctionsTriggeredRun(
                deployer=self.deployer,
                content=json.dumps(
                    {
                        "metadata": self.deployer.metadata,
                        "pathspec": "/".join((self.deployer.flow_name, run_id)),
                        "name": run_id,
                    }
                ),
            )
            triggered_runs.append(triggered_run)

        return triggered_runs

    def delete(self, **kwargs) -> bool:
        """
        Delete the deployed state machine.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize the deletion with a production token.

        Returns
        -------
        bool
            True if the command was successful, False otherwise.
        """
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).delete(**kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        command_obj.sync_wait()
        return command_obj.process.returncode == 0

    def trigger(self, **kwargs) -> CustomStepFunctionsTriggeredRun:
        """
        Trigger a new run for the deployed flow.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments to pass to the trigger command,
            `Parameters` in particular

        Returns
        -------
        CustomStepFunctionsTriggeredRun
            The triggered run instance.

        Raises
        ------
        Exception
            If there is an error during the trigger process.
        """
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            # every subclass needs to have `self.deployer_kwargs`
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(deployer_attribute_file=attribute_file_path, **kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )

            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return CustomStepFunctionsTriggeredRun(
                    deployer=self.deployer, content=content
                )

        raise Exception(
            "Error triggering %s on %s for %s"
            % (
                self.deployer.name,
                self.deployer.TYPE,
                self.deployer.flow_file,
            )
        )
