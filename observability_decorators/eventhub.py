"""Event sink: sends events to Azure Event Hubs / Fabric Eventstream, or prints them if none is configured."""

import json
import os
import time

try:
    from pyspark.sql.types import Row
except ImportError:  # PySpark is optional
    Row = ()

CONNECTION_STRING_ENV_VAR = "EVENTHUB_CONNECTION_STRING"
MAX_BATCH_SIZE_BYTES = 524288


def send_batch(
    payload: list["Row"] | list[dict], event_stream_connection_string: str = ""
):
    """
    Send the payload (list of PySpark Rows or list of dicts or a mix of both)
    to an Event Hub using the connection string.

    The connection string (including 'EntityPath') is taken from the argument or
    from the EVENTHUB_CONNECTION_STRING environment variable. If neither is set,
    the events are printed as JSON instead, which is useful for local demos.
    """
    event_stream_connection_string = event_stream_connection_string or os.environ.get(
        CONNECTION_STRING_ENV_VAR, ""
    )

    if not event_stream_connection_string:
        for row in payload:
            print(json.dumps(row.asDict() if isinstance(row, Row) else row))
        return

    from azure.eventhub import EventHubProducerClient

    producer = EventHubProducerClient.from_connection_string(
        conn_str=event_stream_connection_string
    )

    event_data_batch = producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_BYTES)

    for row in payload:
        try:
            event_data_batch = add_to_batch(row, event_data_batch)
        except ValueError:
            # The batch is full
            producer.send_batch(event_data_batch)
            time.sleep(1)
            event_data_batch = producer.create_batch(
                max_size_in_bytes=MAX_BATCH_SIZE_BYTES
            )
            event_data_batch = add_to_batch(row, event_data_batch)

    if event_data_batch:
        producer.send_batch(event_data_batch)
        print("Sent batch!")
        return

    raise RuntimeError("Something went wrong: batch is empty.")


def add_to_batch(row: "Row | dict", event_data_batch):
    """
    Adds a single row to the batch if it is a PySpark Row or a dict.
    """
    from azure.eventhub import EventData

    if isinstance(row, Row):
        event_data_batch.add(EventData(json.dumps(row.asDict())))
    elif isinstance(row, dict):
        event_data_batch.add(EventData(json.dumps(row)))
    else:
        raise ValueError(
            "Invalid argument to send_batch function: "
            + f"{row} has to be a Python dict or a PySpark Row."
        )
    return event_data_batch
