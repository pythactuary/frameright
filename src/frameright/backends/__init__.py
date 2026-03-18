"""Backend adapters for Schema.

Each backend provides a consistent interface for DataFrame operations,
allowing Schema to work with Pandas, Polars, and other libraries.
"""

from .base import BackendAdapter

__all__ = [
    "BackendAdapter",
]
