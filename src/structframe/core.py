from datetime import date, datetime
from typing import (
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

from .backends.base import BackendAdapter
from .backends.registry import detect_backend, get_backend
from .typing import Col, Index

TStructFrame = TypeVar("TStructFrame", bound="StructFrame")


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
        description: Human-readable description for documentation.
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
        description: Optional[str] = None,
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
        self.description = description

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
            "description",
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
    description: Optional[str] = None,
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
        description: Human-readable description for documentation.

    Returns:
        A FieldInfo metadata object consumed by StructFrame during class creation.
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
        description=description,
    )


class StructFrame:
    """Base class for the Object-DataFrame Mapper (ODM).

    Define your DataFrame schema as a Python class with typed attributes.
    StructFrame validates column existence, runtime dtypes, and field-level
    constraints, while providing IDE-friendly autocomplete and type safety.

    Example::

        class Orders(StructFrame):
            item_price: Col[float]
            quantity_sold: Col[int] = Field(ge=0)
            revenue: Optional[Col[float]]

        orders = Orders(df)
        total = orders.item_price.sum()
    """

    # Stores the parsed schema for the specific child class
    _sf_schema: Dict[str, dict]

    def __init__(
        self,
        df: Any,
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        backend: Optional[str] = None,
    ):
        """Initialise the StructFrame wrapper.

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
            self._sf_backend: BackendAdapter = get_backend(backend)
        else:
            self._sf_backend = detect_backend(df)

        self._sf_df = self._sf_backend.copy(df) if copy else df

        if validate:
            self.sf_validate(check_types=validate_types)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Metaclass hook to parse the schema and inject properties at load time."""
        super().__init_subclass__(**kwargs)
        cls._sf_schema = {}
        cls._sf_index_attrs: List[Dict[str, Any]] = []  # Track Index[T] annotations

        hints = get_type_hints(cls)
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
                    base_schema = getattr(base, "_sf_schema", {})
                    if attr_name in base_schema:
                        field_info = base_schema[attr_name]["field_info"]
                        break
                if field_info is None:
                    field_info = FieldInfo()
                actual_df_col = field_info.alias or attr_name

            # Store the parsed schema for validation later
            cls._sf_schema[attr_name] = {
                "df_col": actual_df_col,
                "inner_type": inner_type,
                "field_info": field_info,
                "is_optional": is_optional,
            }

            # 3. Inject the safe Property wrapper
            def make_property(col_name: str, optional_flag: bool):
                def getter(self: "StructFrame") -> Any:
                    if optional_flag and not self._sf_backend.has_column(self._sf_df, col_name):
                        return None
                    return self._sf_backend.get_column(self._sf_df, col_name)

                def setter(self: "StructFrame", value: Any) -> None:
                    self._sf_df = self._sf_backend.set_column(self._sf_df, col_name, value)

                return property(getter, setter)

            setattr(cls, attr_name, make_property(actual_df_col, is_optional))

        # ------------------------------------------------------------------
        # Inject Index properties (after all hints are collected)
        # ------------------------------------------------------------------
        cls._sf_index_attrs = index_entries

        if len(index_entries) == 1:
            # Single Index — property directly wraps self._sf_df.index
            entry = index_entries[0]

            def make_single_index_property() -> property:
                def getter(self: "StructFrame") -> Any:
                    return self._sf_backend.get_index(self._sf_df)

                def setter(self: "StructFrame", value: Any) -> None:
                    self._sf_df = self._sf_backend.set_index(self._sf_df, value)

                return property(getter, setter)

            setattr(cls, entry["name"], make_single_index_property())

        elif len(index_entries) > 1:
            # MultiIndex — each property accesses its own level
            for entry in index_entries:

                def make_multi_index_property(level_name: str) -> property:
                    def getter(self: "StructFrame") -> Any:
                        return self._sf_backend.get_index_level(self._sf_df, level_name)

                    def setter(self: "StructFrame", value: Any) -> None:
                        self._sf_df = self._sf_backend.set_index_level(
                            self._sf_df, level_name, value
                        )

                    return property(getter, setter)

                setattr(cls, entry["name"], make_multi_index_property(entry["name"]))

    # ------------------------------------------------------------------
    # Core Methods (Prefixed with sf_ to avoid namespace collisions)
    # ------------------------------------------------------------------

    def sf_validate(self, check_types: bool = True) -> "StructFrame":
        """Validate column existence, runtime dtypes, and field-level constraints.

        Uses Pandera for validation, with errors translated into StructFrame
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
        schema = self._sf_backend.build_pandera_schema(
            self.__class__._sf_schema,
            check_types=check_types,
        )
        self._sf_backend.validate_with_pandera(self._sf_df, schema, lazy=True)
        return self

    @property
    def sf_data(self) -> Any:
        """Escape hatch to retrieve the raw underlying DataFrame."""
        return self._sf_df

    @property
    def sf_index(self) -> Any:
        """Access the DataFrame index directly."""
        return self._sf_backend.get_index(self._sf_df)

    @property
    def sf_backend(self) -> BackendAdapter:
        """Access the backend adapter."""
        return self._sf_backend

    def sf_filter(self: TStructFrame, condition: Any) -> TStructFrame:
        """Filter rows and return a new instance of the structured object.

        Args:
            condition: A boolean mask (Series or Polars expression) to apply.

        Returns:
            A new instance of the same StructFrame subclass with filtered rows.
        """
        filtered = self._sf_backend.filter_rows(self._sf_df, condition)
        return self.__class__(filtered, copy=False, validate=False, backend=self._sf_backend.name)

    # ------------------------------------------------------------------
    # Schema Introspection
    # ------------------------------------------------------------------

    @classmethod
    def sf_schema_info(cls, backend: str = "pandas") -> Any:
        """Return a DataFrame describing the schema definition.

        Args:
            backend: Which backend to use for the result DataFrame.
                     Defaults to 'pandas'.

        Returns:
            A DataFrame with columns: attribute, column, type, required,
            nullable, unique, constraints.
        """
        rows = []
        for attr_name, meta in cls._sf_schema.items():
            fi: FieldInfo = meta["field_info"]
            inner = meta["inner_type"]
            constraints = {}
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
                    "description": fi.description,
                }
            )
        adapter = get_backend(backend)
        return adapter.schema_info_to_dataframe(rows)

    # ------------------------------------------------------------------
    # Factory Methods
    # ------------------------------------------------------------------

    @classmethod
    def sf_from_csv(
        cls: Type[TStructFrame],
        path: str,
        backend: str = "pandas",
        **kwargs: Any,
    ) -> TStructFrame:
        """Load a CSV file and wrap it in this StructFrame.

        Args:
            path: File path to the CSV.
            backend: Backend to use for reading ('pandas' or 'polars').
            **kwargs: Additional arguments passed to the backend's CSV reader.

        Returns:
            A validated instance of this StructFrame subclass.
        """
        adapter = get_backend(backend)
        df = adapter.read_csv(path, **kwargs)
        return cls(df, backend=backend)

    @classmethod
    def sf_from_dict(
        cls: Type[TStructFrame],
        data: Dict[str, list],
        backend: str = "pandas",
        **kwargs: Any,
    ) -> TStructFrame:
        """Create from a dictionary of lists.

        Args:
            data: Dictionary mapping column names to lists of values.
            backend: Backend to use ('pandas' or 'polars').
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this StructFrame subclass.
        """
        adapter = get_backend(backend)
        df = adapter.from_dict(data)
        return cls(df, backend=backend, **kwargs)

    @classmethod
    def sf_from_records(
        cls: Type[TStructFrame],
        records: List[dict],
        backend: str = "pandas",
        **kwargs: Any,
    ) -> TStructFrame:
        """Create from a list of row dictionaries.

        Args:
            records: List of dictionaries, one per row.
            backend: Backend to use ('pandas' or 'polars').
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this StructFrame subclass.
        """
        adapter = get_backend(backend)
        df = adapter.from_records(records)
        return cls(df, backend=backend, **kwargs)

    # ------------------------------------------------------------------
    # Type Coercion
    # ------------------------------------------------------------------

    @classmethod
    def sf_coerce(
        cls: Type[TStructFrame],
        df: Any,
        errors: str = "raise",
        backend: Optional[str] = None,
    ) -> TStructFrame:
        """Attempt to convert DataFrame columns to match the schema's type annotations.

        This is useful when loading data from sources that don't preserve dtypes
        (e.g., CSV files where everything is a string).

        Args:
            df: The DataFrame to coerce.
            errors: How to handle conversion errors.
                    'raise' (default), 'coerce' (set failures to NaN), or 'ignore'.
            backend: Explicitly specify backend name. If None, auto-detect.

        Returns:
            A new, validated StructFrame instance with converted dtypes.
        """
        if backend is not None:
            adapter = get_backend(backend)
        else:
            adapter = detect_backend(df)

        df = adapter.copy(df)
        for attr_name, meta in cls._sf_schema.items():
            col = meta["df_col"]
            inner_type = meta["inner_type"]
            if not adapter.has_column(df, col) or inner_type is None:
                continue
            df = adapter.coerce_column(df, col, inner_type, errors=errors)

        return cls(df, backend=adapter.name)

    @classmethod
    def sf_example(
        cls: Type[TStructFrame],
        nrows: int = 3,
        backend: str = "pandas",
    ) -> TStructFrame:
        """Generate an example instance with dummy data for testing.

        Args:
            nrows: Number of rows to generate.
            backend: Backend to use ('pandas' or 'polars').

        Returns:
            An instance populated with simple placeholder data.
        """
        adapter = get_backend(backend)
        df = adapter.generate_example_data(cls._sf_schema, nrows=nrows)
        return cls(df, backend=backend)

    # ------------------------------------------------------------------
    # Export Helpers
    # ------------------------------------------------------------------

    def sf_to_csv(self, path: str, **kwargs: Any) -> None:
        """Save the wrapped DataFrame to a CSV file.

        Args:
            path: File path for the output CSV.
            **kwargs: Additional arguments passed to the backend's CSV writer.
        """
        self._sf_backend.to_csv(self._sf_df, path, **kwargs)

    def sf_to_dict(self, orient: str = "records") -> Any:
        """Convert the wrapped DataFrame to a dictionary.

        Args:
            orient: The format of the output dict (see backend docs).

        Returns:
            A dictionary representation of the data.
        """
        return self._sf_backend.to_dict(self._sf_df, orient=orient)

    # ------------------------------------------------------------------
    # Python Protocols
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self._sf_backend.num_rows(self._sf_df)

    def __repr__(self) -> str:
        schema = self.__class__._sf_schema
        req = sum(1 for m in schema.values() if not m["is_optional"])
        opt = sum(1 for m in schema.values() if m["is_optional"])
        head = self._sf_backend.head(self._sf_df)
        return (
            f"<{self.__class__.__name__} [{self._sf_backend.name}]: "
            f"{len(self)} rows x "
            f"{self._sf_backend.num_cols(self._sf_df)} cols "
            f"({req} required, {opt} optional)>\n"
            f"{head}"
        )

    def __iter__(self):
        """Iterate over rows as named tuples."""
        return self._sf_backend.itertuples(self._sf_df, self.__class__.__name__ + "Row")

    def __eq__(self, other: object) -> bool:
        """Check equality with another StructFrame of the same type."""
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._sf_backend.equals(self._sf_df, other._sf_df)

    def __contains__(self, col_name: str) -> bool:
        """Support ``'col_name' in obj`` syntax."""
        return self._sf_backend.has_column(self._sf_df, col_name)
