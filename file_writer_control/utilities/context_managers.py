from contextlib import contextmanager
import time


@contextmanager
def sleep_in_between(sleep_time: float):
    try:
        yield
    finally:
        time.sleep(sleep_time)
