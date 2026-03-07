"""
ProteusFrame: A lightweight Object-DataFrame Mapper (ODM).

Provides type-safe DataFrame wrappers with runtime validation (via Pandera),
IDE-friendly autocomplete, and Pydantic-style field constraints.
Supports Pandas, Polars, and other DataFrame backends.
"""

from .core import Field, FieldInfo, ProteusFrame
from .exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    SchemaError,
    ProteusFrameError,
    TypeMismatchError,
    ValidationError,
)
from .typing import Col, Index
from .backends.registry import get_backend, detect_backend, register_backend

__version__ = "0.3.0"
__all__ = [
    "ProteusFrame",
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
    "detect_backend",
    "register_backend",
]
