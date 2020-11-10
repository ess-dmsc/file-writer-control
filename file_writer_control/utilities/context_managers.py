from contextlib import contextmanager
import time


@contextmanager
def sleep_in_between(sleep_time: float):
    """
    This manages the sleep times after! task execution.
    """
    try:
        yield  # Execute the body. Any exception will be catched here.
    finally:
        time.sleep(sleep_time)  # Sleep after execution.
