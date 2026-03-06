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

import pandas as pd
import pandas.api.types as ptypes

from .exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    TypeMismatchError,
)
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
        df: pd.DataFrame,
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
    ):
        """Initialise the StructFrame wrapper.

        Args:
            df: The pandas DataFrame to wrap.
            copy: If True, copy the DataFrame. Defaults to False to save memory.
            validate: If True, run schema validation on construction. Defaults to True.
            validate_types: If True, also check runtime dtypes during validation.
                            Only used when ``validate`` is True.
        """
        self._sf_df = df.copy() if copy else df

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
                    if optional_flag and col_name not in self._sf_df.columns:
                        return None
                    return self._sf_df[col_name]

                def setter(self: "StructFrame", value: Any) -> None:
                    self._sf_df[col_name] = value

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
                def getter(self: "StructFrame") -> pd.Index:
                    return self._sf_df.index

                def setter(self: "StructFrame", value: Any) -> None:
                    self._sf_df.index = value

                return property(getter, setter)

            setattr(cls, entry["name"], make_single_index_property())

        elif len(index_entries) > 1:
            # MultiIndex — each property accesses its own level
            for entry in index_entries:

                def make_multi_index_property(level_name: str) -> property:
                    def getter(self: "StructFrame") -> pd.Index:
                        return self._sf_df.index.get_level_values(level_name)

                    def setter(self: "StructFrame", value: Any) -> None:
                        idx = self._sf_df.index
                        arrays = [
                            value if idx.names[i] == level_name else idx.get_level_values(i)
                            for i in range(idx.nlevels)
                        ]
                        self._sf_df.index = pd.MultiIndex.from_arrays(arrays, names=idx.names)

                    return property(getter, setter)

                setattr(cls, entry["name"], make_multi_index_property(entry["name"]))

    # ------------------------------------------------------------------
    # Dtype check mapping (class-level constant, avoid rebuilding per call)
    # ------------------------------------------------------------------
    _SF_DTYPE_CHECKS = {
        int: ptypes.is_integer_dtype,
        float: ptypes.is_float_dtype,
        str: lambda s: ptypes.is_string_dtype(s) or ptypes.is_object_dtype(s),
        bool: ptypes.is_bool_dtype,
        datetime: ptypes.is_datetime64_any_dtype,
        date: ptypes.is_datetime64_any_dtype,
    }

    # ------------------------------------------------------------------
    # Core Methods (Prefixed with sf_ to avoid namespace collisions)
    # ------------------------------------------------------------------

    def sf_validate(self, check_types: bool = True) -> "StructFrame":
        """Validate column existence, runtime dtypes, and field-level constraints.

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
        df = self._sf_df
        actual_cols = set(df.columns)
        missing: List[str] = []
        cls_name = self.__class__.__name__
        dtype_checks = self._SF_DTYPE_CHECKS

        for attr_name, meta in self.__class__._sf_schema.items():
            df_col: str = meta["df_col"]
            inner_type = meta["inner_type"]

            # 1. Check existence
            if df_col not in actual_cols:
                if not meta["is_optional"]:
                    missing.append(df_col)
                continue

            series = df[df_col]
            fi: FieldInfo = meta["field_info"]

            # 2. Check Runtime Dtypes (inlined to avoid per-column method call)
            if check_types and inner_type is not None:
                checker = dtype_checks.get(inner_type)
                if checker is not None and not checker(series):
                    raise TypeMismatchError(
                        f"[{cls_name}] Column '{df_col}' has dtype "
                        f"'{series.dtype}', expected {inner_type.__name__}."
                    )

            # 3. Field-Level Constraint Validations (inlined for performance)
            # Short-circuit: skip entirely if no constraints are set
            if (
                fi.nullable
                and fi.ge is None
                and fi.gt is None
                and fi.le is None
                and fi.lt is None
                and fi.isin is None
                and fi.regex is None
                and fi.min_length is None
                and fi.max_length is None
                and not fi.unique
            ):
                continue

            # Null check (use .hasnans for speed, run first since others may fail on NaN)
            if not fi.nullable and series.hasnans:
                null_count = int(series.isna().sum())
                raise ConstraintViolationError(
                    f"[{cls_name}] Column '{df_col}' contains {null_count} "
                    f"null value(s) but nullable=False."
                )

            # Filter out NaNs for value checks if present
            # If nullable=True, NaNs are allowed and shouldn't trigger constraint violations
            check_series = series.dropna() if (fi.nullable and series.hasnans) else series

            # Numeric constraints (vectorised, very fast)
            if fi.ge is not None and not (check_series >= fi.ge).all():
                raise ConstraintViolationError(f"[{cls_name}] Column '{df_col}' must be >= {fi.ge}")

            if fi.gt is not None and not (check_series > fi.gt).all():
                raise ConstraintViolationError(f"[{cls_name}] Column '{df_col}' must be > {fi.gt}")

            if fi.le is not None and not (check_series <= fi.le).all():
                raise ConstraintViolationError(f"[{cls_name}] Column '{df_col}' must be <= {fi.le}")

            if fi.lt is not None and not (check_series < fi.lt).all():
                raise ConstraintViolationError(f"[{cls_name}] Column '{df_col}' must be < {fi.lt}")

            # Categorical constraint
            if fi.isin is not None and not check_series.isin(fi.isin).all():
                invalid = check_series[~check_series.isin(fi.isin)].unique()[:5]
                raise ConstraintViolationError(
                    f"[{cls_name}] Column '{df_col}' contains invalid values: "
                    f"{invalid.tolist()}. Allowed: {fi.isin}"
                )

            # String constraints: combine to avoid redundant dropna/astype(str)
            has_regex = fi.regex is not None
            has_minlen = fi.min_length is not None
            has_maxlen = fi.max_length is not None

            if has_regex or has_minlen or has_maxlen:
                str_series = series.str

                if has_minlen or has_maxlen:
                    # Use check_series (no NaNs) for length checks
                    check_str_series = check_series.str
                    lengths = check_str_series.len()

                    if has_minlen and not (lengths >= fi.min_length).all():
                        raise ConstraintViolationError(
                            f"[{cls_name}] Column '{df_col}' has values shorter than "
                            f"{fi.min_length} characters."
                        )

                    if has_maxlen and not (lengths <= fi.max_length).all():
                        raise ConstraintViolationError(
                            f"[{cls_name}] Column '{df_col}' has values longer than "
                            f"{fi.max_length} characters."
                        )

                if has_regex:
                    if not str_series.contains(fi.regex, na=True, regex=True).all():
                        # Only compute failures for the error message
                        bad = series[~str_series.contains(fi.regex, na=True, regex=True)]
                        raise ConstraintViolationError(
                            f"[{cls_name}] Column '{df_col}' has values not matching "
                            f"pattern '{fi.regex}'. First failures: {bad.head(3).tolist()}"
                        )

            # Uniqueness constraint
            if fi.unique and series.duplicated().any():
                dup_count = int(series.duplicated().sum())
                raise ConstraintViolationError(
                    f"[{cls_name}] Column '{df_col}' has {dup_count} duplicate "
                    f"value(s) but unique=True."
                )

        if missing:
            raise MissingColumnError(f"[{cls_name}] Missing required columns: {sorted(missing)}")

        return self

    @property
    def sf_data(self) -> pd.DataFrame:
        """Escape hatch to retrieve the raw Pandas DataFrame."""
        return self._sf_df

    @property
    def sf_index(self) -> pd.Index:
        """Access the DataFrame index directly."""
        return self._sf_df.index

    def sf_filter(self: TStructFrame, condition: pd.Series) -> TStructFrame:
        """Filter rows and return a new instance of the structured object.

        Args:
            condition: A boolean Series mask to apply.

        Returns:
            A new instance of the same StructFrame subclass with filtered rows.
        """
        return self.__class__(self._sf_df[condition], copy=False, validate=False)

    # ------------------------------------------------------------------
    # Schema Introspection
    # ------------------------------------------------------------------

    @classmethod
    def sf_schema_info(cls) -> pd.DataFrame:
        """Return a DataFrame describing the schema definition.

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
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Factory Methods
    # ------------------------------------------------------------------

    @classmethod
    def sf_from_csv(cls: Type[TStructFrame], path: str, **kwargs: Any) -> TStructFrame:
        """Load a CSV file and wrap it in this StructFrame.

        Args:
            path: File path to the CSV.
            **kwargs: Additional arguments passed to ``pd.read_csv``.

        Returns:
            A validated instance of this StructFrame subclass.
        """
        df = pd.read_csv(path, **kwargs)
        return cls(df)

    @classmethod
    def sf_from_dict(cls: Type[TStructFrame], data: Dict[str, list], **kwargs: Any) -> TStructFrame:
        """Create from a dictionary of lists.

        Args:
            data: Dictionary mapping column names to lists of values.
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this StructFrame subclass.
        """
        df = pd.DataFrame(data)
        return cls(df, **kwargs)

    @classmethod
    def sf_from_records(
        cls: Type[TStructFrame], records: List[dict], **kwargs: Any
    ) -> TStructFrame:
        """Create from a list of row dictionaries.

        Args:
            records: List of dictionaries, one per row.
            **kwargs: Additional arguments passed to the constructor.

        Returns:
            A validated instance of this StructFrame subclass.
        """
        df = pd.DataFrame.from_records(records)
        return cls(df, **kwargs)

    # ------------------------------------------------------------------
    # Type Coercion
    # ------------------------------------------------------------------

    @classmethod
    def sf_coerce(
        cls: Type[TStructFrame],
        df: pd.DataFrame,
        errors: str = "raise",
    ) -> TStructFrame:
        """Attempt to convert DataFrame columns to match the schema's type annotations.

        This is useful when loading data from sources that don't preserve dtypes
        (e.g., CSV files where everything is a string).

        Args:
            df: The DataFrame to coerce.
            errors: How to handle conversion errors.
                    'raise' (default), 'coerce' (set failures to NaN), or 'ignore'.

        Returns:
            A new, validated StructFrame instance with converted dtypes.
        """
        df = df.copy()
        for attr_name, meta in cls._sf_schema.items():
            col = meta["df_col"]
            inner_type = meta["inner_type"]
            if col not in df.columns or inner_type is None:
                continue

            try:
                if inner_type == int:
                    df[col] = pd.to_numeric(df[col], errors=errors)
                    # Use Int64 for nullable integers consistency
                    try:
                        df[col] = df[col].astype("Int64")
                    except (TypeError, ValueError):
                        # Keep as-is if strictly incompatible (e.g. floats)
                        # The subsequent validate() will catch TypeMismatch if strictly required
                        pass
                elif inner_type == float:
                    df[col] = pd.to_numeric(df[col], errors=errors)
                elif inner_type == str:
                    df[col] = df[col].astype(str)
                elif inner_type == bool:
                    # Safer boolean conversion for strings
                    if ptypes.is_object_dtype(df[col]) or ptypes.is_string_dtype(df[col]):
                        # Cast to object to allow replacing strings with booleans without error
                        df[col] = df[col].astype(object)

                        # Map known string values to booleans
                        s_lower = df[col].astype(str).str.lower()
                        mask_true = s_lower.isin(["true", "1", "yes", "on"])
                        mask_false = s_lower.isin(["false", "0", "no", "off"])

                        df.loc[mask_true, col] = True
                        df.loc[mask_false, col] = False

                    df[col] = df[col].astype(bool)
                elif inner_type in (datetime, date):
                    df[col] = pd.to_datetime(df[col], errors=errors)
            except Exception as e:
                if errors == "raise":
                    raise TypeError(
                        f"[{cls.__name__}] Cannot coerce column '{col}' "
                        f"to {inner_type.__name__}: {e}"
                    ) from e

        return cls(df)

    @classmethod
    def sf_example(cls: Type[TStructFrame], nrows: int = 3) -> TStructFrame:
        """Generate an example instance with dummy data for testing.

        Args:
            nrows: Number of rows to generate.

        Returns:
            An instance populated with simple placeholder data.
        """
        data: Dict[str, list] = {}
        for attr_name, meta in cls._sf_schema.items():
            col = meta["df_col"]
            inner_type = meta["inner_type"]
            if inner_type == int:
                data[col] = list(range(nrows))
            elif inner_type == float:
                data[col] = [float(i) for i in range(nrows)]
            elif inner_type == str:
                data[col] = [f"{attr_name}_{i}" for i in range(nrows)]
            elif inner_type == bool:
                data[col] = [i % 2 == 0 for i in range(nrows)]
            elif inner_type in (datetime, date):
                data[col] = pd.date_range("2020-01-01", periods=nrows, freq="D").tolist()
            else:
                data[col] = [None] * nrows
        return cls(pd.DataFrame(data))

    # ------------------------------------------------------------------
    # Export Helpers
    # ------------------------------------------------------------------

    def sf_to_csv(self, path: str, **kwargs: Any) -> None:
        """Save the wrapped DataFrame to a CSV file.

        Args:
            path: File path for the output CSV.
            **kwargs: Additional arguments passed to ``DataFrame.to_csv``.
        """
        self._sf_df.to_csv(path, index=False, **kwargs)

    def sf_to_dict(self, orient: str = "records") -> Any:
        """Convert the wrapped DataFrame to a dictionary.

        Args:
            orient: The format of the output dict (see ``DataFrame.to_dict``).

        Returns:
            A dictionary representation of the data.
        """
        return self._sf_df.to_dict(orient=orient)

    # ------------------------------------------------------------------
    # Python Protocols
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._sf_df)

    def __repr__(self) -> str:
        schema = self.__class__._sf_schema
        req = sum(1 for m in schema.values() if not m["is_optional"])
        opt = sum(1 for m in schema.values() if m["is_optional"])
        return (
            f"<{self.__class__.__name__}: {len(self)} rows x "
            f"{len(self._sf_df.columns)} cols ({req} required, {opt} optional)>\n"
            f"{self._sf_df.head()}"
        )

    def __iter__(self):
        """Iterate over rows as named tuples."""
        return self._sf_df.itertuples(index=True, name=self.__class__.__name__ + "Row")

    def __eq__(self, other: object) -> bool:
        """Check equality with another StructFrame of the same type."""
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._sf_df.equals(other._sf_df)

    def __contains__(self, col_name: str) -> bool:
        """Support ``'col_name' in obj`` syntax."""
        return col_name in self._sf_df.columns
