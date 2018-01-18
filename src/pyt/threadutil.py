"""
Utility functions for distributing simple operations to multiple threads.
"""


# Imports
# ----------------------------------------


from multiprocessing.pool import ThreadPool


# Typing imports
# ----------------------------------------


from typing import Any, Callable, List, Optional, TypeVar

_T = TypeVar("_T")
_U = TypeVar("_U")


# Metadata
# ------------------------------------------------------------


__author__ = 'Peter Volf'


# Module variables
# ----------------------------------------


_default_pool: ThreadPool = None
"""
The default thread pool.
"""


def get_default_pool() -> ThreadPool:
    """
    Returns the default thread pool.
    """
    global _default_pool
    if _default_pool is None:
        _default_pool = ThreadPool(8)

    return _default_pool


# Public methods
# ----------------------------------------


def batch(items: List[_T], batch_size: int) -> List[List[_T]]:
    """
    Breaks the given list of items into batches of `batch_size` number of items
    and returns the created batches as a list.

    The last batch in the returned list may contain less items than `batch_size`.

    Example:
        >>> print(batch([i for i in range(10)], 3))
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    Arguments:
        items (List[_T]): The items to batch.
        batch_size (int): The desired batch size.

    Returns:
        Batches from the given items with the specified size.
    """
    return [items[i: i + batch_size] for i in range(0, len(items), batch_size)]


def batch_handle(items: List[_T],
                 batch_size: int,
                 batch_handler: Callable[[List[_T]], _U],
                 result_handler: Callable[[_U], Any],
                 pool: Optional[ThreadPool] = None) -> None:
    """
    Breaks the given list of items into batches of the specified size, processes all
    the created batches in a separate thread with `batch_handler` and post-processes
    the return values of `batch_handler` with `result_handler`.

    The last of the created batches may contain less items than `batch_size`.

    Example:
        >>> result: List[int] = []
        >>> batch_handle([i for i in range(0, 71)], 7, sum, result.append, None)
        >>> print(", ".join(str(s) for s in result))
        21, 70, 119, 168, 217, 266, 315, 364, 413, 462, 70

    Arguments:
        items (List[_T]): The items to process in batches.
        batch_size (int): The desired batch size.
        batch_handler (Callable[[List[_T]], _U]): Function that handles one batch of items.
        result_handler (Callable[[_U], Any]): Function that handles the return values of `batch_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    if pool is None:
        pool = get_default_pool()

    for data in pool.imap(batch_handler, batch(items, batch_size)):
        result_handler(data)


def batch_handle_unordered(items: List[_T],
                           batch_size: int,
                           batch_handler: Callable[[List[_T]], _U],
                           result_handler: Callable[[_U], Any],
                           pool: Optional[ThreadPool] = None) -> None:
    """
    Breaks the given list of items into batches of the specified size, processes all
    the created batches in a separate thread with `batch_handler` and post-processes
    the return values of `batch_handler` with `result_handler`.

    The last of the created batches may contain less items than `batch_size`.

    Note that it is not guaranteed that `result_handler` will process the
    return values of `batch_handler` in the order the items were given.

    Example:
        >>> result: List[int] = []
        >>> batch_handle_unordered([i for i in range(0, 71)], 7, sum, result.append, None)
        >>> print(", ".join(str(s) for s in result))
        21, 70, 119, 168, 217, 266, 315, 364, 413, 462, 70

    Arguments:
        items (List[_T]): The items to process in batches.
        batch_size (int): The desired batch size.
        batch_handler (Callable[[List[_T]], _U]): Function that handles one batch of items.
        result_handler (Callable[[_U], Any]): Function that handles the return values of `batch_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    if pool is None:
        pool = get_default_pool()

    for data in pool.imap_unordered(batch_handler, batch(items, batch_size)):
        result_handler(data)


def batch_handle_with_batch(items: List[_T],
                            batch_size: int,
                            batch_handler: Callable[[List[_T]], _U],
                            result_handler: Callable[[List[_T], _U], Any],
                            pool: Optional[ThreadPool] = None) -> None:
    """
    Breaks the given list of items into batches of the specified size, processes all
    the created batches in a separate thread with `batch_handler` and post-processes
    the batch - return value pairs of `batch_handler` with `result_handler`.

    The last of the created batches may contain less items than `batch_size`.

    Example:
        >>> def batch_handler(items: List[int]) -> int:
        ...     return sum(items)
        >>>
        >>> def result_handler(items: List[int], result: int) -> None:
        ...     print("Items: {} | Result: {}".format(", ".join(str(i) for i in items), result))
        >>>
        >>> batch_handle_with_batch([i for i in range(0, 71)], 7, batch_handler, result_handler, None)
        Items: 0, 1, 2, 3, 4, 5, 6 | Result: 21
        Items: 7, 8, 9, 10, 11, 12, 13 | Result: 70
        Items: 14, 15, 16, 17, 18, 19, 20 | Result: 119
        Items: 21, 22, 23, 24, 25, 26, 27 | Result: 168
        Items: 28, 29, 30, 31, 32, 33, 34 | Result: 217
        Items: 35, 36, 37, 38, 39, 40, 41 | Result: 266
        Items: 42, 43, 44, 45, 46, 47, 48 | Result: 315
        Items: 49, 50, 51, 52, 53, 54, 55 | Result: 364
        Items: 56, 57, 58, 59, 60, 61, 62 | Result: 413
        Items: 63, 64, 65, 66, 67, 68, 69 | Result: 462
        Items: 70 | Result: 70

    Arguments:
        items (List[_T]): The items to process in batches.
        batch_size (int): The desired batch size.
        batch_handler (Callable[[List[_T]], _U]): Function that handles one batch of items.
        result_handler (Callable[[List[_T], _U], Any]): Function that handles the batch -
                                                        return value pairs of `batch_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    from functools import partial

    if pool is None:
        pool = get_default_pool()

    for data in pool.imap(partial(_with_item, batch_handler), batch(items, batch_size)):
        result_handler(*data)


def batch_handle_with_batch_unordered(items: List[_T],
                                      batch_size: int,
                                      batch_handler: Callable[[List[_T]], _U],
                                      result_handler: Callable[[List[_T], _U], Any],
                                      pool: Optional[ThreadPool] = None) -> None:
    """
    Breaks the given list of items into batches of the specified size, processes all
    the created batches in a separate thread with `batch_handler` and post-processes
    the batch - return value pairs of `batch_handler` with `result_handler`.

    The last of the created batches may contain less items than `batch_size`.

    Note that it is not guaranteed that `result_handler` will process the
    return values of `batch_handler` in the order the items were given.

    Example:
        >>> def batch_handler(items: List[int]) -> int:
        ...     return sum(items)
        >>>
        >>> def result_handler(items: List[int], result: int) -> None:
        ...     print("Items: {} | Result: {}".format(", ".join(str(i) for i in items), result))
        >>>
        >>> batch_handle_with_batch_unordered([i for i in range(0, 71)], 7, batch_handler, result_handler, None)
        Items: 0, 1, 2, 3, 4, 5, 6 | Result: 21
        Items: 7, 8, 9, 10, 11, 12, 13 | Result: 70
        Items: 14, 15, 16, 17, 18, 19, 20 | Result: 119
        Items: 21, 22, 23, 24, 25, 26, 27 | Result: 168
        Items: 28, 29, 30, 31, 32, 33, 34 | Result: 217
        Items: 35, 36, 37, 38, 39, 40, 41 | Result: 266
        Items: 42, 43, 44, 45, 46, 47, 48 | Result: 315
        Items: 49, 50, 51, 52, 53, 54, 55 | Result: 364
        Items: 56, 57, 58, 59, 60, 61, 62 | Result: 413
        Items: 63, 64, 65, 66, 67, 68, 69 | Result: 462
        Items: 70 | Result: 70

    Arguments:
        items (List[_T]): The items to process in batches.
        batch_size (int): The desired batch size.
        batch_handler (Callable[[List[_T]], _U]): Function that handles one batch of items.
        result_handler (Callable[[List[_T], _U], Any]): Function that handles the batch -
                                                        return value pairs of `batch_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    from functools import partial

    if pool is None:
        pool = get_default_pool()

    for data in pool.imap_unordered(partial(_with_item, batch_handler), batch(items, batch_size)):
        result_handler(*data)


def handle(items: List[_T],
           item_handler: Callable[[_T], _U],
           result_handler: Callable[[_U], Any],
           pool: Optional[ThreadPool] = None) -> None:
    """
    Processes all the items in a separate thread using `item_handler` and
    post-processes the return values of `item_handler` with `result_handler`.

    Example:
        >>> def pow2(x):
        ...   return x**2
        >>> def printer(x, y = "None"):
        ...   print(x, y)
        >>> handle([1, 2, 3], pow2, printer)
        1 None
        4 None
        9 None

    Arguments:
        items (List[_T]): The items to process with `item_handler`.
        item_handler (Callable[[_T], _U]): Function that can handle one item from `items`.
        result_handler (Callable[[_U], Any]): Function that handles the return values of `item_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    if pool is None:
        pool = get_default_pool()

    for data in pool.imap(item_handler, items):
        result_handler(data)


def handle_unordered(items: List[_T],
                     item_handler: Callable[[_T], _U],
                     result_handler: Callable[[_U], Any],
                     pool: Optional[ThreadPool] = None) -> None:
    """
    Processes all the items in a separate thread using `item_handler` and
    post-processes the return values of `item_handler` with `result_handler`.

    Note that it is not guaranteed that `result_handler` will process the
    return values of `item_handler` in the order the items were given.

    Example:
        >>> def pow2(x):
        ...   return x**2
        >>> def printer(x, y = "None"):
        ...   print(x, y)
        >>> handle_unordered([1, 2, 3], pow2, printer)
        1 None
        4 None
        9 None

    Arguments:
        items (List[_T]): The items to process with `item_handler`.
        item_handler (Callable[[_T], _U]): Function that can handle one item from `items`.
        result_handler (Callable[[_U], Any]): Function that handles the return values of `item_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    if pool is None:
        pool = get_default_pool()

    for data in pool.imap_unordered(item_handler, items):
        result_handler(data)


def handle_with_item(items: List[_T],
                     item_handler: Callable[[_T], _U],
                     result_handler: Callable[[_T, _U], Any],
                     pool: Optional[ThreadPool] = None) -> None:
    """
    Processes all the items in a separate thread using `item_handler` and post-processes
    the item - return value pairs of `item_handler` with `result_handler`.

    Example:
        >>> def pow2(x):
        ...   return x**2
        >>> def printer(x, y = "None"):
        ...   print(x, y)
        >>> handle_with_item([1, 2, 3], pow2, printer)
        1 1
        2 4
        3 9

    Arguments:
        items (List[_T]): The items to process with `item_handler`.
        item_handler (Callable[[_T], _U]): Function that can handle one item from `items`.
        result_handler (Callable[[_T, _U], Any]): Function that handles the item - return value pairs of `item_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    from functools import partial

    if pool is None:
        pool = get_default_pool()

    for data in pool.imap_unordered(partial(_with_item, item_handler), items):
        result_handler(*data)


def handle_with_item_unordered(items: List[_T],
                               item_handler: Callable[[_T], _U],
                               result_handler: Callable[[_T, _U], Any],
                               pool: Optional[ThreadPool] = None) -> None:
    """
    Processes all the items in a separate thread using `item_handler` and post-processes
    the item - return value pairs of `item_handler` with `result_handler`.

    Note that it is not guaranteed that `result_handler` will process the
    return values of `item_handler` in the order the items were given.

    Example:
        >>> def pow2(x):
        ...   return x**2
        >>> def printer(x, y = "None"):
        ...   print(x, y)
        >>> handle_with_item_unordered([1, 2, 3], pow2, printer)
        1 1
        2 4
        3 9

    Arguments:
        items (List[_T]): The items to process with `item_handler`.
        item_handler (Callable[[_T], _U]): Function that can handle one item from `items`.
        result_handler (Callable[[_T, _U], Any]): Function that handles the item - return value pairs of `item_handler`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    from functools import partial

    if pool is None:
        pool = get_default_pool()

    for data in pool.imap_unordered(partial(_with_item, item_handler), items):
        result_handler(*data)


def map(items: List[_T], item_handler: Callable[[_T], Any], pool: Optional[ThreadPool] = None) -> None:
    """
    Processes the given items in separate threads with `item_handler.`

    Example:
        >>> def power(data):
        ...     data["x"] = data["x"] ** 2
        ...     data["y"] = data["y"] ** 2
        >>> points = [{"x": i, "y": j} for i in range(4) for j in range(4) if i != j]
        >>> map(points, power)
        >>> print(", ".join([f"({p['x']}, {p['y']})" for p in points]))
        (0, 1), (0, 4), (0, 9), (1, 0), (1, 4), (1, 9), (4, 0), (4, 1), (4, 9), (9, 0), (9, 1), (9, 4)

    Arguments:
        items (List[_T]): The items to process with `item_handler`.
        item_handler (Callable[[_T], Any]): The function to use to process the objects in `items`.
        pool (Optional[ThreadPool]): An optional thread pool to use to process the given items.
                                     If `None`, then the default thread pool will be used.
    """
    if pool is None:
        pool = get_default_pool()

    pool.map(item_handler, items)


# Private methods
# ----------------------------------------


def _with_item(handler: Callable, item: Any):
    """
    Executes `handler` with `item` and returns an `(item, result)` tuple.
    """
    return item, handler(item)
