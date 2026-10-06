from contextvars import ContextVar

from observability_decorators.monitoring import (
    event__notebook_finished_with_success,
    event__notebook_start,
)

_event_context: ContextVar[dict] = ContextVar("_event_context", default={})


def set_initial_event_context(
    notebook_start__event_parent_id,
    notebook_start__event_root_id,
    sinks,
    raw_lakehouse,
    full_rebuild,
    display_output,
    create_partition_column,
    debug,
    pipeline_notebook_start__event_root_id,
    pipeline_notebook_start__event_id,
    workspace_name,
    environment,
    num_rows,
):
    """
    Sets event context for decorators.
    For examples of use, see: observability_decorators.decorators

    """

    event_context = {
        "event_parent_id": notebook_start__event_parent_id,
        "event_root_id": notebook_start__event_root_id,
        "table_name": sinks[0] if sinks else None,
        "lakehouse_name": raw_lakehouse,
        "parameters": {
            "full_rebuild": full_rebuild,
            "display_output": display_output,
            "create_partition_column": create_partition_column,
            "debug": debug,
            "pipeline_notebook_start__event_root_id": pipeline_notebook_start__event_root_id,
            "pipeline_notebook_start__event_id": pipeline_notebook_start__event_id,
        },
        "workspace_name": workspace_name,
        "environment": environment,
        "debug": debug,
        "num_rows": num_rows,
    }
    _event_context.set(event_context)


def update_event_context(**updates):
    """
    Allows updates to the context (e. g. setting num_rows) outside RuntimeConfig.
    For sample usage, see: observability_decorators.etl_helpers.write_delta_table

    """
    current_ctx = _event_context.get() or {}
    new_ctx = dict(current_ctx)
    new_ctx.update(updates)
    _event_context.set(new_ctx)


def send_event_on_start():
    """
    Sends an event using 'event__notebook_start' function
    using a dynamic notebook-specific context.

    """
    event_kwargs = _event_context.get()
    event__notebook_start(**event_kwargs)


def send_event_on_finish_with_success():
    """
    Sends an event using 'event__notebook_finished_with_success' function
    using a dynamic notebook-specific context.

    """
    event_kwargs = _event_context.get()
    event__notebook_finished_with_success(**event_kwargs)
