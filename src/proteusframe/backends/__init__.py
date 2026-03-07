"""Backend adapters for ProteusFrame.

Each backend provides a consistent interface for DataFrame operations,
allowing ProteusFrame to work with Pandas, Polars, and other libraries.
"""

from .base import BackendAdapter
from .registry import detect_backend, get_backend, register_backend

__all__ = [
    "BackendAdapter",
    "detect_backend",
    "get_backend",
    "register_backend",
]
