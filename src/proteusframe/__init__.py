"""
ProteusFrame: A lightweight Object-DataFrame Mapper (ODM).

Provides type-safe DataFrame wrappers with runtime validation (via Pandera),
IDE-friendly autocomplete, and Pydantic-style field constraints.
Supports Pandas, Polars, and other DataFrame backends.
"""

from .backends.registry import get_backend, register_backend
from .core import (
    Field,
    FieldInfo,
    ProteusFrame,
    ProteusFrameNarwhals,
    ProteusFrameNarwhalsLazy,
    ProteusFramePandas,
    ProteusFramePolars,
    ProteusFramePolarsLazy,
)
from .exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    ProteusFrameError,
    SchemaError,
    TypeMismatchError,
    ValidationError,
)
from .typing import Col, Index

__version__ = "0.3.0"
__all__ = [
    "ProteusFrame",
    "ProteusFramePandas",
    "ProteusFramePolars",
    "ProteusFrameNarwhals",
    "ProteusFramePolarsLazy",
    "ProteusFrameNarwhalsLazy",
    "Field",
    "FieldInfo",
    "Col",
    "Index",
    "ProteusFrameError",
    "SchemaError",
    "ValidationError",
    "TypeMismatchError",
    "ConstraintViolationError",
    "MissingColumnError",
    "get_backend",
    "register_backend",
]
