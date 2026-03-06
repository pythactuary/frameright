"""
StructFrame: A lightweight Object-DataFrame Mapper (ODM) for Pandas.

Provides type-safe DataFrame wrappers with runtime validation,
IDE-friendly autocomplete, and Pydantic-style field constraints.
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

__version__ = "0.1.0"
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
]
