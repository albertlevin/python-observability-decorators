"""Defines RuntimeConfig."""

from dataclasses import dataclass
from datetime import datetime

import yaml

from observability_decorators.context import set_initial_event_context
from observability_decorators.helpers import get_execution_time
from observability_decorators.monitoring import create_unique_event_id


@dataclass
class RuntimeConfig:
    """
    Use this class to store runtime config.
    This config is meant to be initialized once at the beginning of a notebook/job.

    """

    environment_config_path: str = "environment.yml"
    full_rebuild: bool = True
    display_output: bool = True
    create_partition_column: bool = True
    debug: bool = True
    pipeline_notebook_start__event_root_id: str | None = None
    pipeline_notebook_start__event_id: str | None = None
    notebook_start__event_id: str | None = None
    notebook_start__event_parent_id: str | None = None
    notebook_start__event_root_id: str | None = None

    # environment variables: start
    environment: str | None = None
    workspace_name: str | None = None
    raw_lakehouse: str | None = None
    # environment variables: end

    execution_time: datetime | None = None
    sources: list | None = None
    sinks: list | None = None

    # this is set at the end of a run
    num_rows: int | None = None

    def set_variables_from_file(self):
        """
        Reads environment variables from a yaml file (see examples/environment.yml).

        Uses the environment_variables to set the rest of the config.
        """

        with open(self.environment_config_path, "r", encoding="utf-8") as file:
            environment_variables = yaml.safe_load(file)

        if self.debug:
            print(
                f"Environment variables from {self.environment_config_path}: \n",
                environment_variables,
            )
        self.environment = environment_variables["environment"]
        self.workspace_name = environment_variables["workspace_name"]
        self.raw_lakehouse = environment_variables["raw_lakehouse"]

        self.execution_time = get_execution_time(debug=self.debug)

        self.notebook_start__event_id = create_unique_event_id(debug=self.debug)
        self.notebook_start__event_parent_id = (
            self.pipeline_notebook_start__event_id
            if self.pipeline_notebook_start__event_id
            else self.notebook_start__event_id
        )
        self.notebook_start__event_root_id = (
            self.pipeline_notebook_start__event_root_id
            if self.pipeline_notebook_start__event_root_id
            else self.notebook_start__event_id
        )

        # after initializing the config, set the context for events/decorators
        set_initial_event_context(
            notebook_start__event_parent_id=self.notebook_start__event_parent_id,
            notebook_start__event_root_id=self.notebook_start__event_root_id,
            sinks=self.sinks,
            raw_lakehouse=self.raw_lakehouse,
            full_rebuild=self.full_rebuild,
            display_output=self.display_output,
            create_partition_column=self.create_partition_column,
            debug=self.debug,
            pipeline_notebook_start__event_root_id=self.pipeline_notebook_start__event_root_id,
            pipeline_notebook_start__event_id=self.pipeline_notebook_start__event_id,
            workspace_name=self.workspace_name,
            environment=self.environment,
            num_rows=self.num_rows,
        )
