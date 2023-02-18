# flake8: noqa

from .measures import *
from .metrics import *

try:
    from ._version import __version__, __version_tuple__
except ImportError:  # pragma: no cover
    __version__ = version = "unknown"
    __version_tuple__ = version_tuple = ("unknown",)

__author__ = "Matthew Wardrop"
__author_email__ = "mpwardrop@gmail.com"
