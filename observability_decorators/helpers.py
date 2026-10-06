"""Helper module to prevent circular imports."""

import datetime


def get_execution_time(debug: bool = False):

    execution_time = datetime.datetime.now()

    if debug:
        print(f"Get execution time| execution_time = {execution_time}")

    return execution_time
