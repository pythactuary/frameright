import ast
import inspect
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
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
from .backends.registry import get_backend
from .exceptions import SchemaError
from .typing import Col, Index

TProteusFrame = TypeVar("TProteusFrame", bound="ProteusFrame")


def _extract_docstrings(cls: Type) -> Dict[str, str]:
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
                            docstrings[attr_name] = next_node.value.value
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
        isin: Optional[list] = None,
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
    isin: Optional[list] = None,
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
        A FieldInfo metadata object consumed by ProteusFrame during class creation.
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


class ProteusFrame:
    """Base class for the Object-DataFrame Mapper (ODM).

    Define your DataFrame schema as a Python class with typed attributes.
    ProteusFrame validates column existence, runtime dtypes, and field-level
    constraints, while providing IDE-friendly autocomplete and type safety.

    **Do not instantiate ProteusFrame directly.** Use backend-specific subclasses:
    - ProteusFramePandas: For pandas DataFrames
    - ProteusFramePolars: For polars eager DataFrames
    - ProteusFramePolarsLazy: For polars LazyFrames
    - ProteusFrameNarwhals: For narwhals DataFrames
    - ProteusFrameNarwhalsLazy: For narwhals LazyFrames

    Example with pandas::

        import pandas as pd
        from proteusframe import ProteusFramePandas

        class Orders(ProteusFramePandas):
            order_id: Col[int]
            revenue: Col[float]

        orders = Orders(pd.DataFrame(...))
        orders.revenue  # Returns pd.Series
        orders.revenue.sum()  # Use pandas methods

    Example with polars::

        import polars as pl
        from proteusframe import ProteusFramePolars

        class Orders(ProteusFramePolars):
            order_id: Col[int]
            revenue: Col[float]

        orders = Orders(pl.DataFrame(...))
        orders.revenue  # Returns pl.Series
        orders.revenue.sum()  # Use polars methods

    Use ``pf_data`` to access the underlying DataFrame.
    """

    # Stores the parsed schema for the specific child class
    _pf_schema: Dict[str, dict]
    _pf_index_attrs: List[Dict[str, Any]]
    _pf_backend_name: Optional[str] = None  # Set by concrete subclasses

    def __init__(
        self,
        df: Any,
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        backend: Optional[str] = None,
    ):
        """Initialise the ProteusFrame wrapper.

        Args:
            df: The DataFrame to wrap.
            copy: If True, copy the DataFrame. Defaults to False to save memory.
            validate: If True, run schema validation on construction. Defaults to True.
            validate_types: If True, also check runtime dtypes during validation.
                            Only used when ``validate`` is True.
            backend: Backend name (e.g. 'pandas', 'polars'). Defaults to 'pandas'.
                     **Recommended:** Use ProteusFramePandas, ProteusFramePolars, etc. instead
                     for better type safety.
        """
        # Determine backend: concrete subclass setting > explicit parameter > default to pandas
        if self._pf_backend_name is not None:
            # Concrete backend-specific class (recommended path)
            if backend is not None and backend != self._pf_backend_name:
                import warnings

                warnings.warn(
                    f"{self.__class__.__name__} is a {self._pf_backend_name} backend class, "
                    f"but backend='{backend}' was passed. Ignoring parameter.",
                    UserWarning,
                    stacklevel=2,
                )
            actual_backend = self._pf_backend_name
        elif backend is not None:
            # Explicit backend parameter
            actual_backend = backend
        else:
            # Default to pandas for backward compatibility
            actual_backend = "pandas"

        self._pf_backend: BackendAdapter = get_backend(actual_backend)
        self._pf_df = self._pf_backend.copy(df) if copy else df

        if validate:
            self.pf_validate(check_types=validate_types)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Metaclass hook to parse the schema and inject properties at load time."""
        super().__init_subclass__(**kwargs)
        cls._pf_schema = {}
        cls._pf_index_attrs = []  # Track Index[T] annotations

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
                    base_schema = getattr(base, "_pf_schema", {})
                    if attr_name in base_schema:
                        field_info = base_schema[attr_name]["field_info"]
                        break
                if field_info is None:
                    field_info = FieldInfo()
                actual_df_col = field_info.alias or attr_name

            # Store the parsed schema for validation later
            cls._pf_schema[attr_name] = {
                "df_col": actual_df_col,
                "inner_type": inner_type,
                "field_info": field_info,
                "is_optional": is_optional,
                "docstring": docstrings.get(attr_name),
            }

            # 3. Inject the safe Property wrapper
            def make_property(col_name: str, optional_flag: bool) -> property:
                def getter(self: "ProteusFrame") -> Any:
                    if optional_flag and not self._pf_backend.has_column(
                        self._pf_df, col_name
                    ):
                        return None

                    # For LazyFrames, use get_column_ref() to return expressions (pl.Expr)
                    # For eager DataFrames, use get_column() to return Series
                    if hasattr(self._pf_backend, "get_column_ref") and hasattr(
                        self._pf_df, "__class__"
                    ):
                        # Check if it's a LazyFrame (polars backend)
                        df_type = type(self._pf_df).__name__
                        if df_type == "LazyFrame":
                            return self._pf_backend.get_column_ref(
                                self._pf_df, col_name
                            )

                    # Return native Series directly (pd.Series, pl.Series, or nw.Series)
                    return self._pf_backend.get_column(self._pf_df, col_name)

                def setter(self: "ProteusFrame", value: Any) -> None:
                    self._pf_df = self._pf_backend.set_column(
                        self._pf_df, col_name, value
                    )

                return property(getter, setter)

            setattr(cls, attr_name, make_property(actual_df_col, is_optional))

        # ------------------------------------------------------------------
        # Inject Index properties (after all hints are collected)
        # ------------------------------------------------------------------
        cls._pf_index_attrs = index_entries

        if len(index_entries) == 1:
            # Single Index — property directly wraps self._pf_df.index
            entry = index_entries[0]

            def make_single_index_property() -> property:
                def getter(self: "ProteusFrame") -> Any:
                    return self._pf_backend.get_index(self._pf_df)

                def setter(self: "ProteusFrame", value: Any) -> None:
                    self._pf_df = self._pf_backend.set_index(self._pf_df, value)

                return property(getter, setter)

            setattr(cls, entry["name"], make_single_index_property())

        elif len(index_entries) > 1:
            # MultiIndex — each property accesses its own level
            for entry in index_entries:

                def make_multi_index_property(level_name: str) -> property:
                    def getter(self: "ProteusFrame") -> Any:
                        return self._pf_backend.get_index_level(self._pf_df, level_name)

                    def setter(self: "ProteusFrame", value: Any) -> None:
                        self._pf_df = self._pf_backend.set_index_level(
                            self._pf_df, level_name, value
                        )

                    return property(getter, setter)

                setattr(cls, entry["name"], make_multi_index_property(entry["name"]))

    # ------------------------------------------------------------------
    # Core Methods (Prefixed with pf_ to avoid namespace collisions)
    # ------------------------------------------------------------------

    def pf_validate(self, check_types: bool = True) -> Self:
        """Validate column existence, runtime dtypes, and field-level constraints.

        Uses Pandera for validation, with errors translated into ProteusFrame
        exception types (MissingColumnError, TypeMismatchError,
        ConstraintViolationError).

        Args:
            check_types: If True, also validate that column dtypes match the
                         type annotations. Defaults to True.

        Returns:
            self, for method chaining.

        Raises:
            MissingColumnError: If a required column is not present.
            TypeMismatchError: If a column's dtype doesn't match the annotation.
            ConstraintViolationError: If a field-level constraint is violated.
        """
        schema = self._pf_backend.build_pandera_schema(
            self.__class__._pf_schema,
            self._pf_df,
            check_types=check_types,
        )
        self._pf_backend.validate_with_pandera(self._pf_df, schema, lazy=True)
        return self

    if TYPE_CHECKING:

        @property
        def pf_data(self) -> "pd.DataFrame":
            """Return the underlying DataFrame.

            For pandas backend, returns ``pd.DataFrame``.
            For polars backend, returns ``pl.DataFrame`` or ``pl.LazyFrame``.
            For narwhals backend, returns ``nw.DataFrame``.

            This property gives direct access to the DataFrame for performing
            operations using the backend's native API::

                # Pandas operations
                df.pf_data.groupby('column').sum()

                # Polars operations
                df.pf_data.filter(pl.col('x') > 5)

                # LazyFrame operations
                lazy_df.pf_data.collect()
            """
            ...

    else:

        @property
        def pf_data(self) -> Any:
            """Return the underlying DataFrame.

            For pandas backend, returns ``pd.DataFrame``.
            For polars backend, returns ``pl.DataFrame`` or ``pl.LazyFrame``.
            For narwhals backend, returns ``nw.DataFrame``.

            This property gives direct access to the DataFrame for performing
            operations using the backend's native API::

                # Pandas operations
                df.pf_data.groupby('column').sum()

                # Polars operations
                df.pf_data.filter(pl.col('x') > 5)

                # LazyFrame operations
                lazy_df.pf_data.collect()
            """
            return self._pf_df

    def __len__(self) -> int:
        """Return the number of rows in the DataFrame.

        For LazyFrames, this will trigger execution (collect) to get the row count.
        If you want to avoid execution, use `.pf_data.collect().shape[0]` instead.
        """
        return self._pf_backend.num_rows(self._pf_df)

    @property
    def pf_index(self) -> Any:
        """Access the DataFrame index directly.

        Returns:
            The DataFrame index (type depends on backend: pandas
            Index/MultiIndex or list for Polars).
        """
        return self._pf_backend.get_index(self._pf_df)

    @property
    def pf_backend(self) -> BackendAdapter:
        """Access the backend adapter."""
        return self._pf_backend

    # ------------------------------------------------------------------
    # Schema Introspection
    # ------------------------------------------------------------------

    @classmethod
    def pf_schema_info(cls) -> List[Dict[str, Any]]:
        """Return the schema definition as a list of dictionaries.

        Returns:
            A list of dicts, one per column, with keys: attribute, column,
            type, required, nullable, unique, constraints, description.
        """
        rows: List[Dict[str, Any]] = []
        for attr_name, meta in cls._pf_schema.items():
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
    # Factory Methods
    # ------------------------------------------------------------------

    @classmethod
    def pf_from_csv(
        cls: Type[TProteusFrame],
        path: str,
        backend: Optional[str] = None,
        **kwargs: Any,
    ) -> TProteusFrame:
        """Load a CSV file and wrap it in this ProteusFrame.

        Args:
            path: File path to the CSV.
            backend: Backend to use ('pandas' or 'polars'). Defaults to 'pandas'.
            **kwargs: Additional arguments passed to the backend's CSV reader.

        Returns:
            A validated instance of this ProteusFrame subclass.
        """
        actual_backend = cls._pf_backend_name or backend or "pandas"
        adapter = get_backend(actual_backend)
        df = adapter.read_csv(path, **kwargs)
        return cls(df, backend=actual_backend)

    @classmethod
    def pf_from_dict(
        cls: Type[TProteusFrame],
        data: Dict[str, list],
        backend: Optional[str] = None,
        **kwargs: Any,
    ) -> TProteusFrame:
        """Create from a dictionary of lists.

        Args:
            data: Dictionary mapping column names to lists of values.
            backend: Backend to use ('pandas' or 'polars'). Defaults to 'pandas'.
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this ProteusFrame subclass.
        """
        actual_backend = cls._pf_backend_name or backend or "pandas"
        adapter = get_backend(actual_backend)
        df = adapter.from_dict(data)
        return cls(df, backend=actual_backend, **kwargs)

    @classmethod
    def pf_from_records(
        cls: Type[TProteusFrame],
        records: List[dict],
        backend: Optional[str] = None,
        **kwargs: Any,
    ) -> TProteusFrame:
        """Create from a list of row dictionaries.

        Args:
            records: List of dictionaries, one per row.
            backend: Backend to use ('pandas' or 'polars'). Defaults to 'pandas'.
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this ProteusFrame subclass.
        """
        actual_backend = cls._pf_backend_name or backend or "pandas"
        adapter = get_backend(actual_backend)
        df = adapter.from_records(records)
        return cls(df, backend=actual_backend, **kwargs)

    # ------------------------------------------------------------------
    # Type Coercion
    # ------------------------------------------------------------------

    @classmethod
    def pf_coerce(
        cls: Type[TProteusFrame],
        df: Any,
        errors: str = "raise",
        backend: Optional[str] = None,
    ) -> TProteusFrame:
        """Attempt to convert DataFrame columns to match the schema's type annotations.

        This is useful when loading data from sources that don't preserve dtypes
        (e.g., CSV files where everything is a string).

        Args:
            df: The DataFrame to coerce.
            errors: How to handle conversion errors.
                    'raise' (default), 'coerce' (set failures to NaN), or 'ignore'.
            backend: Backend name. Defaults to 'pandas'.

        Returns:
            A new, validated ProteusFrame instance with converted dtypes.
        """
        actual_backend = cls._pf_backend_name or backend or "pandas"
        adapter = get_backend(actual_backend)

        df = adapter.copy(df)
        for attr_name, meta in cls._pf_schema.items():
            col = meta["df_col"]
            inner_type = meta["inner_type"]
            field_info = meta["field_info"]
            if not adapter.has_column(df, col) or inner_type is None:
                continue
            df = adapter.coerce_column(
                df, col, inner_type, errors=errors, nullable=field_info.nullable
            )

        return cls(df, backend=actual_backend)

    @classmethod
    def pf_example(
        cls: Type[TProteusFrame],
        nrows: int = 3,
        backend: Optional[str] = None,
    ) -> TProteusFrame:
        """Generate an example instance with dummy data for testing.

        Args:
            nrows: Number of rows to generate.
            backend: Backend to use ('pandas' or 'polars'). Defaults to 'pandas'.

        Returns:
            An instance populated with simple placeholder data.
        """
        actual_backend = cls._pf_backend_name or backend or "pandas"
        adapter = get_backend(actual_backend)
        df = adapter.generate_example_data(cls._pf_schema, nrows=nrows)
        return cls(df, backend=actual_backend)

    # ------------------------------------------------------------------
    # Python Protocols
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        schema = self.__class__._pf_schema
        req = sum(1 for m in schema.values() if not m["is_optional"])
        opt = sum(1 for m in schema.values() if m["is_optional"])
        head = self._pf_backend.head(self._pf_df)
        return (
            f"<{self.__class__.__name__} [{self._pf_backend.name}]: "
            f"{len(self)} rows x "
            f"{self._pf_backend.num_cols(self._pf_df)} cols "
            f"({req} required, {opt} optional)>\n"
            f"{head}"
        )

    def __iter__(self) -> Any:
        """Iterate over rows as named tuples."""
        return self._pf_backend.itertuples(self._pf_df, self.__class__.__name__ + "Row")

    def __eq__(self, other: object) -> bool:
        """Check equality with another ProteusFrame of the same type."""
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._pf_backend.equals(self._pf_df, other._pf_df)

    def __contains__(self, col_name: str) -> bool:
        """Support ``'col_name' in obj`` syntax.

        Checks both Python attribute names and raw DataFrame column names.
        This ensures that with aliases, both work: 'tier' in users and
        'customer_tier' in users.
        """
        # Check if it's a Python attribute name in the schema
        if col_name in self._pf_schema:
            return True
        # Fall back to checking raw DataFrame column name
        return self._pf_backend.has_column(self._pf_df, col_name)


# ------------------------------------------------------------------
# Concrete Backend-Specific Classes
# ------------------------------------------------------------------


class ProteusFramePandas(ProteusFrame):
    """ProteusFrame for pandas DataFrames.

    Use this when working with pandas:

        import pandas as pd
        from proteusframe import ProteusFramePandas

        class Sales(ProteusFramePandas):
            customer: Col[str]
            revenue: Col[float]

        df = pd.DataFrame({"customer": ["Alice"], "revenue": [100.0]})
        sales = Sales(df)
        sales.revenue  # Returns pd.Series
    """

    _pf_backend_name = "pandas"

    if TYPE_CHECKING:

        @property
        def pf_data(self) -> "pd.DataFrame":
            """Return the underlying pandas DataFrame."""
            ...


class ProteusFramePolars(ProteusFrame):
    """ProteusFrame for polars eager DataFrames.

    Use this when working with polars DataFrames:

        import polars as pl
        from proteusframe import ProteusFramePolars

        class Sales(ProteusFramePolars):
            customer: Col[str]
            revenue: Col[float]

        df = pl.DataFrame({"customer": ["Alice"], "revenue": [100.0]})
        sales = Sales(df)
        sales.revenue  # Returns pl.Series
    """

    _pf_backend_name = "polars"

    if TYPE_CHECKING:

        @property
        def pf_data(self) -> "pl.DataFrame":
            """Return the underlying polars DataFrame."""
            ...


class ProteusFramePolarsLazy(ProteusFrame):
    """ProteusFrame for polars LazyFrames.

    Use this when working with polars LazyFrames:

        import polars as pl
        from proteusframe import ProteusFramePolarsLazy

        class Sales(ProteusFramePolarsLazy):
            customer: Col[str]
            revenue: Col[float]

        df = pl.DataFrame({"customer": ["Alice"], "revenue": [100.0]}).lazy()
        sales = Sales(df)
        sales.revenue  # Returns pl.Expr (lazy expression)
    """

    _pf_backend_name = "polars"

    if TYPE_CHECKING:

        @property
        def pf_data(self) -> "pl.LazyFrame":
            """Return the underlying polars LazyFrame."""
            ...


class ProteusFrameNarwhals(ProteusFrame):
    """ProteusFrame for narwhals DataFrames.

    Use this when working with narwhals eager DataFrames:

        import narwhals as nw
        from proteusframe import ProteusFrameNarwhals

        class Sales(ProteusFrameNarwhals):
            customer: Col[str]
            revenue: Col[float]

        df = nw.from_native(pd_or_pl_df)
        sales = Sales(df)
        sales.revenue  # Returns nw.Series
    """

    _pf_backend_name = "narwhals"

    if TYPE_CHECKING:

        @property
        def pf_data(self) -> "nw.DataFrame":
            """Return the underlying narwhals DataFrame."""
            ...


class ProteusFrameNarwhalsLazy(ProteusFrame):
    """ProteusFrame for narwhals LazyFrames.

    Use this when working with narwhals lazy DataFrames:

        import narwhals as nw
        from proteusframe import ProteusFrameNarwhalsLazy

        class Sales(ProteusFrameNarwhalsLazy):
            customer: Col[str]
            revenue: Col[float]

        df = nw.from_native(pl.LazyFrame(...))
        sales = Sales(df)
        sales.revenue  # Returns nw.Expr (lazy expression)
    """

    _pf_backend_name = "narwhals"

    if TYPE_CHECKING:

        @property
        def pf_data(self) -> "nw.LazyFrame":
            """Return the underlying narwhals LazyFrame."""
            ...
