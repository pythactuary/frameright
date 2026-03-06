"""
StructFrame: A lightweight Object-DataFrame Mapper (ODM).

Provides type-safe DataFrame wrappers with runtime validation (via Pandera),
IDE-friendly autocomplete, and Pydantic-style field constraints.
Supports Pandas, Polars, and other DataFrame backends.
"""

from .core import Field, FieldInfo, StructFrame
from .exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    SchemaError,
    StructFrameError,
    TypeMismatchError,
    ValidationError,
)
from .typing import Col, Index
from .backends.registry import get_backend, detect_backend, register_backend

__version__ = "0.3.0"
__all__ = [
    "StructFrame",
    "Field",
    "FieldInfo",
    "Col",
    "Index",
    "StructFrameError",
    "SchemaError",
    "ValidationError",
    "TypeMismatchError",
    "ConstraintViolationError",
    "MissingColumnError",
    "get_backend",
    "detect_backend",
    "register_backend",
]
