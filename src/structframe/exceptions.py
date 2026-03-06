"""Custom exceptions for StructFrame."""


class StructFrameError(Exception):
    """Base exception for all StructFrame errors."""

    pass


class SchemaError(StructFrameError):
    """Raised when a schema definition is invalid."""

    pass


class ValidationError(StructFrameError):
    """Raised when DataFrame validation fails."""

    pass


class TypeMismatchError(ValidationError):
    """Raised when a column's dtype doesn't match the expected type."""

    pass


class ConstraintViolationError(ValidationError):
    """Raised when a field-level constraint is violated."""

    pass


class MissingColumnError(ValidationError):
    """Raised when a required column is missing from the DataFrame."""

    pass
