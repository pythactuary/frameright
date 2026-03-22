"""
FrameRight: A lightweight Object-DataFrame Mapper (ODM).

Provides type-safe DataFrame wrappers with runtime validation (via Pandera),
IDE-friendly autocomplete, and Pydantic-style field constraints.
Supports Pandas, Polars, and other DataFrame backends.
"""

from .core import Field, FieldInfo
from .exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    SchemaError,
    StructFrameError,
    TypeMismatchError,
    ValidationError,
)
from .typing import Col

__version__ = "0.3.0"
__all__ = [
    "Field",
    "FieldInfo",
    "Col",
    "StructFrameError",
    "SchemaError",
    "ValidationError",
    "TypeMismatchError",
    "ConstraintViolationError",
    "MissingColumnError",
]
