"""
Local demo: no Spark and no Azure needed. Events are printed as JSON.

Run from the repository root:  python examples/demo.py
"""

from observability_decorators import (
    RuntimeConfig,
    send_event_on_error,
    send_event_on_finish_with_success,
    send_event_on_start,
    update_event_context,
)
from observability_decorators.context import _event_context

# 1. Initialise the runtime config once; this also fills the event context.
config = RuntimeConfig(
    environment_config_path="examples/environment.yml",
    sources=["orders_raw"],
    sinks=["orders_clean"],
    debug=False,
)
config.set_variables_from_file()


# 2. Existing functions are extended with a decorator; their bodies stay unchanged.
@send_event_on_error(_event_context)
def clean_orders(rows):
    update_event_context(num_rows=len(rows))
    return [r for r in rows if r["amount"] > 0]


@send_event_on_error(_event_context)
def broken_step(rows):
    return rows[0]["missing_column"]


if __name__ == "__main__":
    send_event_on_start()

    orders = [{"amount": 10}, {"amount": -1}, {"amount": 25}]
    clean_orders(orders)
    send_event_on_finish_with_success()

    try:
        broken_step(orders)
    except KeyError:
        print("broken_step failed; an error event was sent before the exception propagated")
