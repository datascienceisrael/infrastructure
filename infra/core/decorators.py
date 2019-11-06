import functools
from time import time
from typing import Any, Callable

from configuration import config
from infra.core.gcp.gcl import gcl_log_event


def measure_time(func: Callable) -> Any:
    """
    Measure the run time of the decorated function and logs it to
    Google Cloud Stack Driver.

    Args:
        func (Callable): The function to decorate.

    Returns:
        [Any]: the return value of the decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        rv = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        msg = f'The function {func.__name__} completed in {run_time:.4f} ' +\
            f'secs.'
        gcl_log_event(logger_name=config.LOGGER_NAME,
                      event_name='Time Measurement',
                      message=msg,
                      functionName=func.__name__,
                      runTime=run_time)
        return rv
    return wrapper
