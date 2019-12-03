import functools
from time import time
from typing import Any, Callable

import pandas as pd

from infra.core.dataframe import get_statistics_df, save_dataframes
from infra.core.logging import log_event


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
        log_event(event_name='Time Measurement',
                  message=msg,
                  functionName=func.__name__,
                  runTime=run_time)

        return rv
    return wrapper


def visualization(base_path):
    """
    Saving function input and output, and their statistics in csv files into a
    specific path, in order to allow further visulaisation of the data fllow.

    Args:
        base_path (Callable): Base path to save csv files.

    Returns:
        [Any]: the return value of the decorated function.
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            df_output = None
            for arg in args:
                if isinstance(arg, pd.DataFrame):
                    df_input = arg.copy()
                    df_output = f(*args, **kwargs)
                    stats_df_input = get_statistics_df(df_input)
                    stats_df_output = get_statistics_df(df_output)
                    df_names = ['input', 'output',
                                'stats_input', 'stats_output']
                    save_dataframes(df_names, [
                                    df_input, df_output, stats_df_input,
                                    stats_df_output], base_path, f.__name__)
                    break

            return df_output
        return wrapper
    return decorator
