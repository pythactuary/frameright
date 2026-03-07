import ast
import inspect
import sys
from datetime import date, datetime
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

import pandas as pd  # Required dependency (via pandera)

if TYPE_CHECKING:
    import polars as pl

from .backends.base import BackendAdapter
from .backends.registry import detect_backend, get_backend
from .exceptions import SchemaError
from .typing import Col, Index

DFType = TypeVar("DFType", default=pd.DataFrame)
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


def Field(
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


class ProteusFrame(Generic[DFType]):
    """Base class for the Object-DataFrame Mapper (ODM).

    Define your DataFrame schema as a Python class with typed attributes.
    ProteusFrame validates column existence, runtime dtypes, and field-level
    constraints, while providing IDE-friendly autocomplete and type safety.

    Type-parameterize for full IDE support on ``pf_data``::

        class Orders(ProteusFrame[pd.DataFrame]):
            item_price: Col[float]
            quantity_sold: Col[int] = Field(ge=0)
            revenue: Optional[Col[float]]

        orders = Orders(df)
        orders.pf_data.groupby(...)  # Full pd.DataFrame autocomplete

    The type parameter defaults to ``pd.DataFrame``, so omitting it
    gives the same autocomplete::

        class Orders(ProteusFrame):  # pf_data → pd.DataFrame
            ...

    For Polars, specify explicitly::

        class Orders(ProteusFrame[pl.DataFrame]):  # pf_data → pl.DataFrame
            ...
    """

    # Stores the parsed schema for the specific child class
    _pf_schema: Dict[str, dict]
    _pf_index_attrs: List[Dict[str, Any]]

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
            df: The DataFrame to wrap (pandas, polars, or other supported backend).
            copy: If True, copy the DataFrame. Defaults to False to save memory.
            validate: If True, run schema validation on construction. Defaults to True.
            validate_types: If True, also check runtime dtypes during validation.
                            Only used when ``validate`` is True.
            backend: Explicitly specify backend name (e.g. 'pandas', 'polars').
                     If None, auto-detect from the DataFrame type.
        """
        if backend is not None:
            self._pf_backend: BackendAdapter = get_backend(backend)
        else:
            self._pf_backend = detect_backend(df)

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
                    if optional_flag and not self._pf_backend.has_column(self._pf_df, col_name):
                        return None
                    # For eager DataFrames: return Series (materialized data)
                    # For LazyFrames: return Expr (lazy evaluation)
                    try:
                        return self._pf_backend.get_column(self._pf_df, col_name)
                    except TypeError:
                        # LazyFrame case - return expression instead
                        return self._pf_backend.get_column_ref(self._pf_df, col_name)

                def setter(self: "ProteusFrame", value: Any) -> None:
                    self._pf_df = self._pf_backend.set_column(self._pf_df, col_name, value)

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

    def pf_validate(self, check_types: bool = True) -> "ProteusFrame":
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
            check_types=check_types,
        )
        self._pf_backend.validate_with_pandera(self._pf_df, schema, lazy=True)
        return self

    @property
    def pf_data(self) -> DFType:
        """Escape hatch to retrieve the raw underlying DataFrame.

        Returns:
            The underlying DataFrame with full IDE autocomplete.
            Defaults to ``pd.DataFrame`` when no type parameter is given.
            Specify ``ProteusFrame[pl.DataFrame]`` for Polars autocomplete.

        Example::

            class Orders(ProteusFrame):
                revenue: Col[float]

            orders = Orders(df)
            orders.pf_data.groupby(...)  # Full pd.DataFrame autocomplete
        """
        return self._pf_df

    @property
    def pf_index(self) -> Any:
        """Access the DataFrame index directly.

        Returns:
            The DataFrame index (type depends on backend: pandas Index/MultiIndex or list for Polars).
        """
        return self._pf_backend.get_index(self._pf_df)

    @property
    def pf_backend(self) -> BackendAdapter:
        """Access the backend adapter."""
        return self._pf_backend

    def pf_filter(self: TProteusFrame, condition: Any) -> TProteusFrame:
        """Filter rows and return a new instance of the structured object.

        Args:
            condition: A boolean mask (Series or Polars expression) to apply.

        Returns:
            A new instance of the same ProteusFrame subclass with filtered rows.
        """
        filtered = self._pf_backend.filter_rows(self._pf_df, condition)
        return self.__class__(filtered, copy=False, validate=False, backend=self._pf_backend.name)

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def pf_collect(self: TProteusFrame) -> TProteusFrame:
        """Materialise a lazy backend (e.g. Polars LazyFrame → DataFrame).

        For eager backends (Pandas, Polars DataFrame) this returns *self*
        unchanged.  For Polars LazyFrames the query plan is executed and
        a new ProteusFrame wrapping the collected DataFrame is returned.

        Returns:
            A ProteusFrame backed by an eager DataFrame.
        """
        collected = self._pf_backend.collect(self._pf_df)
        if collected is self._pf_df:
            return self
        return self.__class__(collected, copy=False, validate=False, backend=self._pf_backend.name)

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
            for key in ["ge", "gt", "le", "lt", "isin", "regex", "min_length", "max_length"]:
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
        backend: str = "pandas",
        **kwargs: Any,
    ) -> TProteusFrame:
        """Load a CSV file and wrap it in this ProteusFrame.

        Args:
            path: File path to the CSV.
            backend: Backend to use for reading ('pandas' or 'polars').
            **kwargs: Additional arguments passed to the backend's CSV reader.

        Returns:
            A validated instance of this ProteusFrame subclass.
        """
        adapter = get_backend(backend)
        df = adapter.read_csv(path, **kwargs)
        return cls(df, backend=backend)

    @classmethod
    def pf_from_dict(
        cls: Type[TProteusFrame],
        data: Dict[str, list],
        backend: str = "pandas",
        **kwargs: Any,
    ) -> TProteusFrame:
        """Create from a dictionary of lists.

        Args:
            data: Dictionary mapping column names to lists of values.
            backend: Backend to use ('pandas' or 'polars').
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this ProteusFrame subclass.
        """
        adapter = get_backend(backend)
        df = adapter.from_dict(data)
        return cls(df, backend=backend, **kwargs)

    @classmethod
    def pf_from_records(
        cls: Type[TProteusFrame],
        records: List[dict],
        backend: str = "pandas",
        **kwargs: Any,
    ) -> TProteusFrame:
        """Create from a list of row dictionaries.

        Args:
            records: List of dictionaries, one per row.
            backend: Backend to use ('pandas' or 'polars').
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this ProteusFrame subclass.
        """
        adapter = get_backend(backend)
        df = adapter.from_records(records)
        return cls(df, backend=backend, **kwargs)

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
            backend: Explicitly specify backend name. If None, auto-detect.

        Returns:
            A new, validated ProteusFrame instance with converted dtypes.
        """
        if backend is not None:
            adapter = get_backend(backend)
        else:
            adapter = detect_backend(df)

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

        return cls(df, backend=adapter.name)

    @classmethod
    def pf_example(
        cls: Type[TProteusFrame],
        nrows: int = 3,
        backend: str = "pandas",
    ) -> TProteusFrame:
        """Generate an example instance with dummy data for testing.

        Args:
            nrows: Number of rows to generate.
            backend: Backend to use ('pandas' or 'polars').

        Returns:
            An instance populated with simple placeholder data.
        """
        adapter = get_backend(backend)
        df = adapter.generate_example_data(cls._pf_schema, nrows=nrows)
        return cls(df, backend=backend)

    # ------------------------------------------------------------------
    # Export Helpers
    # ------------------------------------------------------------------

    def pf_to_csv(self, path: str, **kwargs: Any) -> None:
        """Save the wrapped DataFrame to a CSV file.

        Args:
            path: File path for the output CSV.
            **kwargs: Additional arguments passed to the backend's CSV writer.
        """
        self._pf_backend.to_csv(self._pf_df, path, **kwargs)

    def pf_to_dict(self, orient: str = "records") -> Any:
        """Convert the wrapped DataFrame to a dictionary.

        Args:
            orient: The format of the output dict (see backend docs).

        Returns:
            A dictionary representation of the data.
        """
        return self._pf_backend.to_dict(self._pf_df, orient=orient)

    # ------------------------------------------------------------------
    # Python Protocols
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self._pf_backend.num_rows(self._pf_df)

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
