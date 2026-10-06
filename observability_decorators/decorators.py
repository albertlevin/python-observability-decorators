"""
Contains send_event_on_error and other decorators.
Use decorators from this module to extend the behaviour of existing functions.

"""

import functools
from contextvars import ContextVar

from observability_decorators.monitoring import event__notebook_finished_with_error


def send_event_on_error(ctx: ContextVar):
    """
    Sends an event using 'event__notebook_finished_with_error' function
    if the wrapped function fails.
    ctx passes a dynamic notebook-specific context to the decorator.

    The context is a dictionary defined in the notebook:

    event_kwargs = {
        "event_parent_id":  notebook_start__event_parent_id,
        "event_root_id":  notebook_start__event_root_id,
        "table_name":  sinks[0],
        "lakehouse_name":  raw_lakehouse,
        "parameters":  {
            "full_rebuild": full_rebuild,
            "display_output": display_output,
            "create_partition_column": create_partition_column,
            "debug": debug,
            "pipeline_notebook_start__event_root_id": pipeline_notebook_start__event_root_id,
            "pipeline_notebook_start__event_id": pipeline_notebook_start__event_id
        },
        "workspace_name":  workspace_name,
        "environment":  environment,
        "debug":  debug
    }
    """

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            event_kwargs = ctx.get()
            try:
                result = function(*args, **kwargs)
            except Exception as e:  # pylint: disable=W0703
                # report the failure, then re-raise so callers still see the error
                event__notebook_finished_with_error(**event_kwargs)
                raise e
            return result

        return wrapper

    return decorator
