import ast
import inspect
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import pandas as pd

if TYPE_CHECKING:
    import narwhals as nw  # noqa: F401
    import pandas as pd  # noqa: F401
    import polars as pl  # noqa: F401

from .backends.base import BackendAdapter
from .exceptions import SchemaError
from .typing import Col, Index

TStructFrame = TypeVar("TStructFrame", bound="BaseSchema")


def _extract_docstrings(cls: Type[Any]) -> Dict[str, str]:
    """Extract field docstrings from class source code using AST parsing.

    Parses the class source to find string literals that immediately follow
    annotated assignments, which Python treats as field docstrings.

    Args:
        cls: The class to extract docstrings from.

    Returns:
        A dictionary mapping attribute names to their docstrings.
    """
    try:
        import textwrap

        source = inspect.getsource(cls)
        # Dedent to handle classes defined inside functions/methods
        source = textwrap.dedent(source)
        tree = ast.parse(source)
        # Find the ClassDef node (should be the first statement)
        classdef = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classdef = node
                break

        if classdef is None:
            return {}

        docstrings = {}
        for i, node in enumerate(classdef.body):
            # Look for annotated assignments (e.g., x: int = Field())
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                attr_name = node.target.id
                # Check if next node is a string expression (docstring)
                if i + 1 < len(classdef.body):
                    next_node = classdef.body[i + 1]
                    if isinstance(next_node, ast.Expr):
                        # Python 3.8+ uses ast.Constant for string literals
                        if isinstance(next_node.value, ast.Constant) and isinstance(
                            next_node.value.value, str
                        ):
                            docstrings[attr_name] = str(next_node.value.value)
                        # Legacy Python (ast.Str)
                        elif isinstance(next_node.value, ast.Str):
                            docstrings[attr_name] = next_node.value.s
        return docstrings
    except (OSError, TypeError, IndentationError, SyntaxError):
        # Can't get source (e.g., in REPL, dynamically created class, or parse error)
        return {}


class FieldInfo:
    """Stores metadata for column mapping and field-level validation.

    Attributes:
        alias: Map this attribute to a differently-named DataFrame column.
        ge: Value must be greater than or equal to this.
        gt: Value must be strictly greater than this.
        le: Value must be less than or equal to this.
        lt: Value must be strictly less than this.
        isin: Value must be one of these allowed values.
        regex: String value must match this regex pattern.
        min_length: String value must be at least this long.
        max_length: String value must be at most this long.
        nullable: Whether NaN/None values are allowed (default True).
        unique: Whether all values must be unique (default False).
    """

    def __init__(
        self,
        alias: Optional[str] = None,
        ge: Optional[float] = None,
        gt: Optional[float] = None,
        le: Optional[float] = None,
        lt: Optional[float] = None,
        isin: Optional[List[Any]] = None,
        regex: Optional[str] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        nullable: bool = True,
        unique: bool = False,
    ):
        self.alias = alias
        self.ge = ge
        self.gt = gt
        self.le = le
        self.lt = lt
        self.isin = isin
        self.regex = regex
        self.min_length = min_length
        self.max_length = max_length
        self.nullable = nullable
        self.unique = unique

    def __repr__(self) -> str:
        parts = []
        for attr in [
            "alias",
            "ge",
            "gt",
            "le",
            "lt",
            "isin",
            "regex",
            "min_length",
            "max_length",
            "nullable",
            "unique",
        ]:
            val = getattr(self, attr)
            # Skip defaults
            if attr == "nullable" and val is True:
                continue
            if attr == "unique" and val is False:
                continue
            if val is not None:
                parts.append(f"{attr}={val!r}")
        return f"Field({', '.join(parts)})" if parts else "Field()"


def Field(  # noqa: N802 (function name should be lowercase)
    alias: Optional[str] = None,
    ge: Optional[float] = None,
    gt: Optional[float] = None,
    le: Optional[float] = None,
    lt: Optional[float] = None,
    isin: Optional[List[Any]] = None,
    regex: Optional[str] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    nullable: bool = True,
    unique: bool = False,
) -> Any:
    """Helper function to define a field's properties and constraints.

    Args:
        alias: Map this attribute to a differently-named DataFrame column.
        ge: Value must be >= this threshold.
        gt: Value must be > this threshold.
        le: Value must be <= this threshold.
        lt: Value must be < this threshold.
        isin: Value must be one of these allowed values.
        regex: String value must match this regex pattern.
        min_length: String value must be at least this long.
        max_length: String value must be at most this long.
        nullable: Whether NaN/None values are allowed (default True).
        unique: Whether all values must be unique (default False).

    Returns:
        A FieldInfo metadata object consumed by Schema during class creation.
    """
    return FieldInfo(
        alias=alias,
        ge=ge,
        gt=gt,
        le=le,
        lt=lt,
        isin=isin,
        regex=regex,
        min_length=min_length,
        max_length=max_length,
        nullable=nullable,
        unique=unique,
    )


class BaseSchema:
    """Base class for the Object-DataFrame Mapper (ODM).

    Define your DataFrame schema as a Python class with typed attributes.
    Schema validates column existence, runtime dtypes, and field-level
    constraints, while providing IDE-friendly autocomplete and type safety.

    **Do not instantiate BaseSchema directly.** Use backend-specific Schema classes:

        from frameright.pandas import Schema  # For pandas
        from frameright.polars.eager import Schema  # For polars eager
        from frameright.polars.lazy import Schema  # For polars lazy

    Example with pandas::

        import pandas as pd
        from frameright.pandas import Schema, Col

        class Orders(Schema):
            order_id: Col[int]
            revenue: Col[float]

        orders = Orders(pd.DataFrame(...))
        orders.revenue  # Returns pd.Series
        orders.revenue.sum()  # Use pandas methods

    Example with polars::

        import polars as pl
        from frameright.polars.eager import Schema, Col

        class Orders(Schema):
            order_id: Col[int]
            revenue: Col[float]

        orders = Orders(pl.DataFrame(...))
        orders.revenue  # Returns pl.Series
        orders.revenue.sum()  # Use polars methods

    Use ``fr_data`` to access the underlying DataFrame.
    """

    # Stores the parsed schema for the specific child class
    _fr_schema: Dict[str, Dict[str, Any]]
    _fr_index_attrs: List[Dict[str, Any]]
    _fr_backend: BackendAdapter  # Set by concrete subclasses (must be non-None)

    def __init__(
        self,
        df: Any,
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        coerce: bool = False,
        coerce_errors: str = "raise",
        strict: bool = False,
    ):
        """Initialise the Schema wrapper.

        Args:
            df: The DataFrame to wrap.
            copy: If True, copy the DataFrame. Defaults to False to save memory.
            validate: If True, run schema validation on construction. Defaults to True.
            validate_types: If True, also check runtime dtypes during validation.
                            Only used when ``validate`` is True.
            coerce: If True, attempt to convert DataFrame columns to match the schema's
                    type annotations before validation. Useful for data from sources
                    that don't preserve dtypes (e.g., CSV files). Defaults to False.
            coerce_errors: How to handle coercion errors when ``coerce`` is True.
                          'raise' (default), 'coerce' (set failures to NaN), or 'ignore'.
            strict: If True, reject DataFrames with columns not defined in the schema.
                    Defaults to False (extra columns are allowed).
        """
        # Concrete subclasses must set _fr_backend at class level
        if not hasattr(self.__class__, "_fr_backend") or self._fr_backend is None:
            raise RuntimeError(
                f"{self.__class__.__name__} must set _fr_backend. "
                "Use a backend-specific class like frameright.pandas.Schema"
            )

        self._fr_df = self._fr_backend.copy(df) if copy else df

        # Apply type coercion if requested
        if coerce:
            for attr_name, meta in self.__class__._fr_schema.items():
                col = meta["df_col"]
                inner_type = meta["inner_type"]
                field_info = meta["field_info"]
                if (
                    not self._fr_backend.has_column(self._fr_df, col)
                    or inner_type is None
                ):
                    continue
                self._fr_df = self._fr_backend.coerce_column(
                    self._fr_df,
                    col,
                    inner_type,
                    errors=coerce_errors,
                    nullable=field_info.nullable,
                )

        if validate:
            self.fr_validate(check_types=validate_types, strict=strict)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Metaclass hook to parse the schema and inject properties at load time."""
        super().__init_subclass__(**kwargs)
        cls._fr_schema = {}
        cls._fr_index_attrs = []  # Track Index[T] annotations

        # Extract docstrings from class source code
        docstrings = _extract_docstrings(cls)

        # Resolve type hints with Col/Index injected into the namespace so
        # that ``from __future__ import annotations`` and TYPE_CHECKING-guarded
        # imports both work without NameError at runtime.
        module = sys.modules.get(cls.__module__)
        globalns = dict(vars(module)) if module else {}
        localns: Dict[str, Any] = {"Col": Col, "Index": Index}
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        index_entries: List[Dict[str, Any]] = []

        for attr_name, attr_type in hints.items():
            if attr_name.startswith("_"):
                continue

            # Collect Index[T] annotations (property injection deferred)
            idx_origin = get_origin(attr_type)
            if idx_origin is Index:
                idx_inner = get_args(attr_type)[0] if get_args(attr_type) else None
                index_entries.append({"name": attr_name, "inner_type": idx_inner})
                continue

            # 1. Parse Type Hints (Handle Col[T] and Optional[Col[T]])
            origin = get_origin(attr_type)
            args = get_args(attr_type)

            is_optional = origin is Union and type(None) in args

            # Extract the actual Col[T] if it was wrapped in Optional
            col_type = (
                next((a for a in args if get_origin(a) is Col), attr_type)
                if is_optional
                else attr_type
            )

            # Validate that the annotation is Col[T] or Optional[Col[T]]
            if get_origin(col_type) is not Col:
                raise SchemaError(
                    f"Attribute '{attr_name}' in {cls.__name__} must be annotated as "
                    f"Col[T] or Optional[Col[T]], got {attr_type}"
                )

            # Extract the inner primitive type (e.g., float from Col[float])
            inner_type = (
                get_args(col_type)[0]
                if get_origin(col_type) is Col and get_args(col_type)
                else None
            )

            # Handle Union types inside Col (e.g., Col[str | None] -> str)
            if inner_type is not None and get_origin(inner_type) is Union:
                union_args = get_args(inner_type)
                inner_type = next((t for t in union_args if t is not type(None)), None)

            # 2. Parse Field Metadata (Alias and Validation constraints)
            class_var = getattr(cls, attr_name, None)
            if isinstance(class_var, FieldInfo):
                field_info = class_var
                actual_df_col = field_info.alias or attr_name
            else:
                # Check parent classes for inherited FieldInfo
                field_info = None
                for base in cls.__mro__[1:]:
                    base_schema = getattr(base, "_fr_schema", {})
                    if attr_name in base_schema:
                        field_info = base_schema[attr_name]["field_info"]
                        break
                if field_info is None:
                    field_info = FieldInfo()
                actual_df_col = field_info.alias or attr_name

            # Store the parsed schema for validation later
            cls._fr_schema[attr_name] = {
                "df_col": actual_df_col,
                "inner_type": inner_type,
                "field_info": field_info,
                "is_optional": is_optional,
                "docstring": docstrings.get(attr_name),
            }

            # 3. Inject the safe Property wrapper
            def make_property(col_name: str, optional_flag: bool) -> property:
                def getter(self: "BaseSchema") -> Any:
                    if optional_flag and not self._fr_backend.has_column(
                        self._fr_df, col_name
                    ):
                        return None

                    # For LazyFrames, use get_column_ref() to return expressions (pl.Expr)
                    # For eager DataFrames, use get_column() to return Series
                    if hasattr(self._fr_backend, "get_column_ref") and hasattr(
                        self._fr_df, "__class__"
                    ):
                        # Check if it's a LazyFrame (polars backend)
                        df_type = type(self._fr_df).__name__
                        if df_type == "LazyFrame":
                            return self._fr_backend.get_column_ref(
                                self._fr_df, col_name
                            )

                    # Return native Series directly (pd.Series, pl.Series, or nw.Series)
                    return self._fr_backend.get_column(self._fr_df, col_name)

                def setter(self: "BaseSchema", value: Any) -> None:
                    self._fr_df = self._fr_backend.set_column(
                        self._fr_df, col_name, value
                    )

                return property(getter, setter)

            setattr(cls, attr_name, make_property(actual_df_col, is_optional))

        # ------------------------------------------------------------------
        # Inject Index properties (after all hints are collected)
        # ------------------------------------------------------------------
        cls._fr_index_attrs = index_entries

        if len(index_entries) == 1:
            # Single Index — property directly wraps self._fr_df.index
            entry = index_entries[0]

            def make_single_index_property() -> property:
                def getter(self: "BaseSchema") -> Any:
                    return self._fr_backend.get_index(self._fr_df)

                def setter(self: "BaseSchema", value: Any) -> None:
                    self._fr_df = self._fr_backend.set_index(self._fr_df, value)

                return property(getter, setter)

            setattr(cls, entry["name"], make_single_index_property())

        elif len(index_entries) > 1:
            # MultiIndex — each property accesses its own level
            for entry in index_entries:

                def make_multi_index_property(level_name: str) -> property:
                    def getter(self: "BaseSchema") -> Any:
                        return self._fr_backend.get_index_level(self._fr_df, level_name)

                    def setter(self: "BaseSchema", value: Any) -> None:
                        self._fr_df = self._fr_backend.set_index_level(
                            self._fr_df, level_name, value
                        )

                    return property(getter, setter)

                setattr(cls, entry["name"], make_multi_index_property(entry["name"]))

    # ------------------------------------------------------------------
    # Core Methods (Prefixed with fr_ to avoid namespace collisions)
    # ------------------------------------------------------------------

    def fr_validate(self, check_types: bool = True, strict: bool = False) -> Self:
        """Validate column existence, runtime dtypes, and field-level constraints.

        Uses Pandera for validation, with errors translated into Schema
        exception types (MissingColumnError, TypeMismatchError,
        ConstraintViolationError).

        Args:
            check_types: If True, also validate that column dtypes match the
                         type annotations. Defaults to True.
            strict: If True, reject DataFrames with columns not defined in the schema.
                    Defaults to False (extra columns are allowed).

        Returns:
            self, for method chaining.

        Raises:
            MissingColumnError: If a required column is not present.
            TypeMismatchError: If a column's dtype doesn't match the annotation.
            ConstraintViolationError: If a field-level constraint is violated.
        """
        schema = self._fr_backend.build_pandera_schema(
            self.__class__._fr_schema,
            self._fr_df,
            check_types=check_types,
            strict=strict,
        )
        self._fr_backend.validate_with_pandera(self._fr_df, schema, lazy=True)
        return self

    @property
    def fr_data(self) -> Any:
        """Return the underlying DataFrame.

        For pandas backend, returns ``pd.DataFrame``.
        For polars backend, returns ``pl.DataFrame`` or ``pl.LazyFrame``.
        For narwhals backend, returns ``nw.DataFrame``.

        This property gives direct access to the DataFrame for performing
        operations using the backend's native API::

            # Pandas operations
            df.fr_data.groupby('column').sum()

            # Polars operations
            df.fr_data.filter(pl.col('x') > 5)

            # LazyFrame operations
            lazy_df.fr_data.collect()
        """
        return self._fr_df

    def __len__(self) -> int:
        """Return the number of rows in the DataFrame.

        For LazyFrames, this will trigger execution (collect) to get the row count.
        If you want to avoid execution, use `.fr_data.collect().shape[0]` instead.
        """
        return self._fr_backend.num_rows(self._fr_df)

    @property
    def fr_backend(self) -> BackendAdapter:
        """Access the backend adapter."""
        return self._fr_backend

    # ------------------------------------------------------------------
    # Schema Introspection
    # ------------------------------------------------------------------

    @classmethod
    def fr_schema_info(cls) -> List[Dict[str, Any]]:
        """Return the schema definition as a list of dictionaries.

        Returns:
            A list of dicts, one per column, with keys: attribute, column,
            type, required, nullable, unique, constraints, description.
        """
        rows: List[Dict[str, Any]] = []
        for attr_name, meta in cls._fr_schema.items():
            fi: FieldInfo = meta["field_info"]
            inner = meta["inner_type"]
            constraints: Dict[str, Any] = {}
            for key in [
                "ge",
                "gt",
                "le",
                "lt",
                "isin",
                "regex",
                "min_length",
                "max_length",
            ]:
                val = getattr(fi, key, None)
                if val is not None:
                    constraints[key] = val

            rows.append(
                {
                    "attribute": attr_name,
                    "column": meta["df_col"],
                    "type": inner.__name__ if inner else "Any",
                    "required": not meta["is_optional"],
                    "nullable": fi.nullable,
                    "unique": fi.unique,
                    "constraints": constraints or None,
                    "description": meta.get("docstring"),
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Python Protocols
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        schema = self.__class__._fr_schema
        req = sum(1 for m in schema.values() if not m["is_optional"])
        opt = sum(1 for m in schema.values() if m["is_optional"])
        head = self._fr_backend.head(self._fr_df)
        return (
            f"<{self.__class__.__name__} [{self._fr_backend.name}]: "
            f"{len(self)} rows x "
            f"{self._fr_backend.num_cols(self._fr_df)} cols "
            f"({req} required, {opt} optional)>\n"
            f"{head}"
        )

    def __iter__(self) -> Any:
        """Iterate over rows as named tuples."""
        return self._fr_backend.itertuples(self._fr_df, self.__class__.__name__ + "Row")

    def __eq__(self, other: object) -> bool:
        """Check equality with another Schema of the same type."""
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._fr_backend.equals(self._fr_df, other._fr_df)

    def __contains__(self, col_name: str) -> bool:
        """Support ``'col_name' in obj`` syntax.

        Checks both Python attribute names and raw DataFrame column names.
        This ensures that with aliases, both work: 'tier' in users and
        'customer_tier' in users.
        """
        # Check if it's a Python attribute name in the schema
        if col_name in self._fr_schema:
            return True
        # Fall back to checking raw DataFrame column name
        return self._fr_backend.has_column(self._fr_df, col_name)
