"""Module for event-related functions."""

import datetime
import json
import uuid
import warnings

from observability_decorators.eventhub import send_batch


def create_event_metadata(
    event_type_version, workspace_name, environment, is_debug, debug=False
):
    """Create common metadata for events."""

    metadata = {
        "event_version": 1,
        "event_type_version": event_type_version,
        "workspace_name": workspace_name,
        "environment": environment,
        "is_debug": is_debug,
    }

    if debug:
        print(metadata)

    return metadata


def create_unique_event_id(debug=False):
    """Implements unique ID for events."""

    # Generate UUID4
    unique_id = uuid.uuid4()

    # Generate current timestamp
    current_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Concatenate UUID4 with timestamp
    event_id = f"{current_timestamp}_{unique_id}"

    if debug:
        print(f"Generate unique event ID: event_id = {event_id}")

    return event_id


# added 'kwargs' so that that all events can accept the same parameters (e. g. num_rows),
# even if these are not used in the event itself.
def event__notebook_start(
    event_parent_id,
    event_root_id,
    table_name,
    lakehouse_name,
    parameters,
    workspace_name,
    environment,
    debug=False,
    **kwargs,
):
    """Sends 'notebook_start' event."""

    event_id = create_unique_event_id(debug)
    event_type = "Notebook_Start"
    event_status = "In Progress"
    metadata = create_event_metadata(
        event_type_version=1,
        workspace_name=workspace_name,
        environment=environment,
        is_debug=debug,
        debug=debug,
    )

    payload = {
        "table": table_name,
        "lakehouse": lakehouse_name,
        "parameters": json.dumps(parameters),
    }

    execution_time = datetime.datetime.now().isoformat()
    event = {
        "event_id": event_id,
        "event_parent_id": event_parent_id,
        "event_root_id": event_root_id,
        "event_timestamp": execution_time,
        "event_type": event_type,
        "event_status": event_status,
        "event_version": 1,
        "metadata": metadata,
        "payload": payload,
    }

    if debug:
        print(event)

    send_batch(payload=[event])

    return event


# added 'kwargs' so that that all events can accept the same parameters (e. g. num_rows),
# even if these are not used in the event itself.
def event__notebook_finished_with_success(
    event_parent_id,
    event_root_id,
    table_name,
    lakehouse_name,
    parameters,
    workspace_name,
    environment,
    debug=False,
    **kwargs,
) -> None:
    """Sends 'Notebook_Finished_with_Success' event."""

    num_rows = kwargs.get("num_rows", None)
    if not num_rows:
        warnings.warn(
            "event__notebook_finished_with_success | expected num_rows, got None. Using fallback: num_rows = 0",
            RuntimeWarning,
        )
        num_rows = 0
    event_id = create_unique_event_id(debug)
    event_type = "Notebook_Finished_with_Success"
    event_status = "Success"
    metadata = create_event_metadata(
        event_type_version=1,
        workspace_name=workspace_name,
        environment=environment,
        is_debug=debug,
        debug=debug,
    )

    payload = {"table": table_name, "lakehouse": lakehouse_name, "row_count": num_rows}

    execution_time = datetime.datetime.now().isoformat()
    event = {
        "event_id": event_id,
        "event_parent_id": event_parent_id,
        "event_root_id": event_root_id,
        "event_timestamp": execution_time,
        "event_type": event_type,
        "event_status": event_status,
        "event_version": 1,
        "metadata": metadata,
        "payload": payload,
    }

    if debug:
        print(event)

    send_batch(payload=[event])


# added 'kwargs' so that that all events can accept the same parameters (e. g. num_rows),
# even if these are not used in the event itself.
def event__notebook_finished_with_error(
    event_parent_id,
    event_root_id,
    table_name,
    lakehouse_name,
    parameters,
    workspace_name,
    environment,
    debug=False,
    **kwargs,
):
    """Sends 'notebook_finished_with_error' event."""

    event_id = create_unique_event_id(debug)
    event_type = "Notebook_Finished_with_Error"
    event_status = "Error"
    metadata = create_event_metadata(
        event_type_version=1,
        workspace_name=workspace_name,
        environment=environment,
        is_debug=debug,
        debug=debug,
    )

    payload = {
        "table": table_name,
        "lakehouse": lakehouse_name,
        "parameters": json.dumps(parameters),
    }

    execution_time = datetime.datetime.now().isoformat()
    event = {
        "event_id": event_id,
        "event_parent_id": event_parent_id,
        "event_root_id": event_root_id,
        "event_timestamp": execution_time,
        "event_type": event_type,
        "event_status": event_status,
        "event_version": 1,
        "metadata": metadata,
        "payload": payload,
    }

    if debug:
        print(event)

    send_batch(payload=[event])

    return event
