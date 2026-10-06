"""
Sample PySpark helpers showing how existing functions are extended with
'send_event_on_error' without touching their bodies.

Requires pyspark (pip install ".[spark]").
"""

from pyspark.sql.functions import lit

from observability_decorators.context import _event_context, update_event_context
from observability_decorators.decorators import send_event_on_error


@send_event_on_error(_event_context)
def read_delta_table(
    spark_session,
    lakehouse_name: str,
    table_name: str,
    sources: list,
    debug: bool = False,
):

    if debug:
        print(lakehouse_name, table_name, sources)

    if table_name not in sources:
        raise ValueError(f"Table '{table_name}' missing in sources")

    return spark_session.read.table(lakehouse_name + "." + table_name)


@send_event_on_error(_event_context)
def drop_table(spark_session, lakehouse: str, table_name: str, debug: bool = False):

    combined_table_name = lakehouse + "." + table_name

    if debug:
        print("Dropping Table " + combined_table_name)

    spark_session.sql(f"DROP TABLE IF EXISTS {combined_table_name}")


@send_event_on_error(_event_context)
def write_delta_table(
    dataframe,
    lakehouse,
    table_name: str,
    load_timestamp_column: str,
    execution_timestamp,
    sinks: list,
    debug: bool = False,
):

    if sinks is not None and table_name not in sinks:
        raise ValueError(f"Table '{table_name}' missing in sinks")

    row_count = dataframe.count()
    if row_count > 0:
        output = dataframe.withColumn(load_timestamp_column, lit(execution_timestamp))
        output.write.mode("overwrite").format("delta").saveAsTable(
            lakehouse + "." + table_name
        )
    else:
        output = None
        print("No new data available")

    # the row count is picked up by the 'finished with success' event
    update_event_context(num_rows=row_count)
    return output
