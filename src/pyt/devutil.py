"""
All sorts of development utilities.
"""


# Imports
# ----------------------------------------


from contextlib import contextmanager

from functools import wraps

from time import time


# Typing imports
# ----------------------------------------


from typing import Callable, Optional


# Metadata
# ------------------------------------------------------------


__author__ = 'Peter Volf'


# Public methods
# ----------------------------------------


def fuzz(min_delay: float = 0.1, max_delay: float = 1.1) -> None:
    """
    Delays execution for a random number of seconds from the `[min_delay, max_delay]` interval.

    Arguments:
        min_delay (float): The minimum amount of delay in seconds.
        max_delay (float): The maximum amount of delay in seconds.
    """
    from random import uniform
    from time import sleep

    sleep(uniform(min_delay, max_delay))


def repeated_timed_method(repeats: int = 10,
                          identifier: Optional[str] = None,
                          printer: Callable[[str], None] = print) -> Callable:
    """
    Decorator that executes the decorated method in a `timed_context` with the given arguments
    the specified number of times and prints the execution statistics using the specified
    printer function.

    The decorated method will return the result of the last execution of the wrapped method.

    Arguments:
        repeats (int): The number of times to repeat the decorated method.
        identifier (Optional[str]): An optional prefix to add to the output.
        printer (Callable[[str], None]): The method to use to print the execution statistics.

    Returns:
        The decorated method.
    """
    def decorator(method):

        @wraps(method)
        def wrapped(*args, **kwargs):
            result = None
            times = []

            for _ in range(repeats):
                with timed_context(times.append, None):
                    result = method(*args, **kwargs)

            entries = []
            if identifier is None:
                indent = ""
            else:
                indent = "  - "
                entries.append(f"Execution statistics of {identifier}:")
            total = round(sum(times), 4)
            entries.append(f"{indent}Shortest execution time: {min(times)} seconds.")
            entries.append(f"{indent}Average execution time: {round(total/len(times), 4)} seconds.")
            entries.append(f"{indent}Longest execution time: {max(times)} seconds.")
            entries.append(f"{indent}Total execution time: {total} seconds.")
            printer("\n".join(entries))
            return result

        return wrapped

    wraps(decorator)
    return decorator


@contextmanager
def timed_context(handler: Callable = print,
                  formatter: Optional[Callable] = "Completed in {} seconds".format) -> None:
    """
    Context manager that can be used to time code execution.

    Arguments:
        handler (Callable): The method to call with the measured (and formatted) duration.
        formatter (Optional[Callable]): Optional method that formats the measured
                                        duration and returns the formatted value.
    """
    start: float = time()
    yield None  # This is where the content of the with block is executed.
    start = round(time() - start, 4)
    if formatter is not None:
        handler(formatter(start))
    else:
        handler(start)


def timed_method(handler: Callable = print,
                 formatter: Optional[Callable] = "Completed in {} seconds".format) -> Callable:
    """
    Decorator that executes the decorated method in a `timed_context` with the given arguments.

    Arguments:
        handler (Callable): The method to call with the measured (and formatted) call duration.
        formatter (Optional[Callable]): Optional method that formats the measured call
                                        duration and returns the formatted value.

    Returns:
        The decorated method.
    """
    def decorator(method: Callable):
        @wraps(method)
        def wrapped(*args, **kwargs):
            with timed_context(handler, formatter):
                return method(*args, **kwargs)
        return wrapped

    wraps(decorator)
    return decorator
