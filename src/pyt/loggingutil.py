"""
Logging related utilities.

The module must be initialized before being used by calling its `initialize()`
method with the desired root logger name.
"""


# Imports
# ----------------------------------------


import logging


# Metadata
# ------------------------------------------------------------


__author__ = 'Peter Volf'


# Module variables
# ----------------------------------------


_root_name: str = None
"""
The name of the root logger.
"""


# Errors
# ----------------------------------------


class AlreadyInitialized(Exception):
    """
    Raised when someone tries to initialize the module when it has already been initialized.
    """
    pass


class NotInitialized(Exception):
    """
    Raised when someone tries to use a component that required initialization but that
    has not been performed yet.
    """
    pass


# Public methods
# ----------------------------------------


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger for the given name that will always be a child of the root logger.

    Arguments:
        name (str): The name of the logger to return. The name should not
                    include the supposed name of the root logger, that
                    will be prepended to it appropriately.

    Raises:
        NotInitialized: If the module hasn't been initialized yet.
    """
    logger: logging.Logger = get_root_logger()
    return logger.getChild(name)


def get_root_logger() -> logging.Logger:
    """
    Returns the root logger.

    Raises:
        NotInitialized: If the module hasn't been initialized yet.
    """
    global _root_name
    if _root_name is None:
        raise NotInitialized("Logging hasn't been initialized!")

    return logging.getLogger(_root_name)


def initialize(root_name: str, add_console_handler: bool = True, add_file_handler: bool = False) -> None:
    """
    Initializes the module with the specified root logger name.

    Initialization must take place exactly once, otherwise an error will be raised.

    Arguments:
        root_name (str): The name of the root logger.
        add_console_handler (bool): Whether a console log handler should be added to the root logger.
        add_file_handler (bool): Whether a file log handler should be added to the root logger.

    Raises:
        AlreadyInitialized: If the module has already been initialized.
    """
    global _root_name
    if _root_name is not None:
        raise AlreadyInitialized("Logging has already been initialized.")

    if root_name is None or len(root_name) <= 2:
        raise ValueError("The root logger name is too short or None.")

    _root_name = root_name

    # Create the root logger.
    logger: logging.Logger = logging.getLogger(root_name)

    # Set the desired log level.
    logger.setLevel(logging.DEBUG)

    # Create te log formatter to use.
    formatter: logging.Formatter = logging.Formatter(
        "%(levelname)s | %(asctime)s\n - %(name)s\n - %(message)s"
    )

    # Console handler configuration.
    if add_console_handler:
        handler: logging.Handler = logging.StreamHandler()
        # Set the log level and the formatter.
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        # Add the handler to the logger.
        logger.addHandler(handler)

    # File handler configuration.
    if add_file_handler:
        handler = logging.FileHandler(root_name + ".log")
        # Set the log level and the formatter.
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        # Add the handler to the logger.
        logger.addHandler(handler)


# Logging methods
# ----------------------------------------


def debug(msg, *args, **kwargs) -> None:
    """
    Convenience method for logging a debug message.

    This call is equivalent to calling `get_root_logger().debug(msg, *args, **kwargs)`.
    """
    get_root_logger().debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs) -> None:
    """
    Convenience method for logging an info message.

    This call is equivalent to calling `get_root_logger().info(msg, *args, **kwargs)`.
    """
    get_root_logger().debug(msg, *args, **kwargs)


def warning(msg, *args, **kwargs) -> None:
    """
    Convenience method for logging a warning message.

    This call is equivalent to calling `get_root_logger().warning(msg, *args, **kwargs)`.
    """
    get_root_logger().warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs) -> None:
    """
    Convenience method for logging an error message.

    This call is equivalent to calling `get_root_logger().error(msg, *args, **kwargs)`.
    """
    get_root_logger().error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs) -> None:
    """
    Convenience method for logging an exception message.

    This call is equivalent to calling `get_root_logger().exception(msg, *args, **kwargs)`.
    """
    get_root_logger().exception(msg, *args, **kwargs)
