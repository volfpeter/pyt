"""
Generic errors.
"""


# Metadata
# ------------------------------------------------------------


__author__ = 'Peter Volf'


# Classes
# ------------------------------------------------------------


class AlreadyInitialized(Exception):
    """
    Raised when someone tries to initialize a component that has already been initialized.
    """
    pass


class NotInitialized(Exception):
    """
    Raised when someone tries to use a component that required initialization but that
    has not been performed yet.
    """
    pass
