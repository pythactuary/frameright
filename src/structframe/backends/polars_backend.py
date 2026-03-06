"""Polars backend adapter for StructFrame."""

from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type

from ..exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    TypeMismatchError,
)
from .base import BackendAdapter

try:
    import polars as pl

    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False


def _require_polars() -> None:
    if not _HAS_POLARS:
        raise ImportError(
            "Polars is required for the Polars backend. "
            "Install it with: pip install structframe[polars]"
        )


class PolarsBackend(BackendAdapter):
    """Backend adapter for Polars DataFrames."""

    def __init__(self) -> None:
        _require_polars()

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "polars"

    # ------------------------------------------------------------------
    # Dtype mapping (Python type → Polars dtype)
    # ------------------------------------------------------------------

    _POLARS_DTYPE_MAP: Dict[type, Any] = {}

    @classmethod
    def _ensure_dtype_map(cls) -> None:
        """Lazily populate the dtype map (needs polars imported)."""
        if not cls._POLARS_DTYPE_MAP and _HAS_POLARS:
            cls._POLARS_DTYPE_MAP = {
                int: pl.Int64,
                float: pl.Float64,
                str: pl.Utf8,
                bool: pl.Boolean,
                datetime: pl.Datetime,
                date: pl.Date,
            }

    # ------------------------------------------------------------------
    # DataFrame operations
    # ------------------------------------------------------------------

    def copy(self, df: "pl.DataFrame | pl.LazyFrame") -> "pl.DataFrame | pl.LazyFrame":
        if isinstance(df, pl.LazyFrame):
            return df  # LazyFrames are immutable query plans; no copy needed
        return df.clone()

    def get_column(self, df: "pl.DataFrame", col: str) -> "pl.Series":
        if isinstance(df, pl.LazyFrame):
            raise TypeError(
                "Cannot materialise a column from a LazyFrame. "
                "Use get_column_ref() to obtain a pl.col() expression instead."
            )
        return df[col]

    def get_column_ref(self, df: Any, col: str) -> "pl.Expr":
        """Return a lazy ``pl.col()`` expression — preserves the query optimizer."""
        return pl.col(col)

    def set_column(
        self, df: "pl.DataFrame | pl.LazyFrame", col: str, value: Any
    ) -> "pl.DataFrame | pl.LazyFrame":
        if isinstance(value, pl.Expr):
            new_col = value.alias(col)
        elif isinstance(value, pl.Series):
            new_col = value.alias(col)  # type: ignore[assignment]
        else:
            new_col = pl.lit(value).alias(col)
        return df.with_columns(new_col)

    def has_column(self, df: "pl.DataFrame | pl.LazyFrame", col: str) -> bool:
        if isinstance(df, pl.LazyFrame):
            return col in df.columns
        return col in df.columns

    def column_names(self, df: "pl.DataFrame | pl.LazyFrame") -> List[str]:
        return df.columns

    def num_rows(self, df: "pl.DataFrame | pl.LazyFrame") -> int:
        if isinstance(df, pl.LazyFrame):
            return df.collect().height
        return df.height

    def num_cols(self, df: "pl.DataFrame | pl.LazyFrame") -> int:
        if isinstance(df, pl.LazyFrame):
            return df.collect_schema().len()
        return df.width

    # ------------------------------------------------------------------
    # Index operations (Polars has no native index concept)
    # ------------------------------------------------------------------

    def get_index(self, df: "pl.DataFrame") -> Any:
        # Polars doesn't have an index; return a row-number series
        return pl.Series("index", range(df.height))

    def set_index(self, df: "pl.DataFrame", value: Any) -> "pl.DataFrame":
        # No-op for Polars (no index concept). Stored as a column instead.
        if isinstance(value, pl.Series):
            s = value.alias("_index")
        else:
            s = pl.Series("_index", value)
        return df.with_columns(s)

    def get_index_level(self, df: "pl.DataFrame", level_name: str) -> Any:
        # Multi-index not natively supported; use column fallback
        if level_name in df.columns:
            return df[level_name]
        raise KeyError(f"No column '{level_name}' found (Polars has no MultiIndex)")

    def set_index_level(self, df: "pl.DataFrame", level_name: str, value: Any) -> "pl.DataFrame":
        return self.set_column(df, level_name, value)  # type: ignore[return-value]

    def index_nlevels(self, df: "pl.DataFrame") -> int:
        # Polars doesn't support multi-level indices
        return 1

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_rows(
        self, df: "pl.DataFrame | pl.LazyFrame", mask: Any
    ) -> "pl.DataFrame | pl.LazyFrame":
        if isinstance(mask, pl.Expr):
            return df.filter(mask)
        if isinstance(mask, pl.Series):
            if isinstance(df, pl.LazyFrame):
                # Convert Series mask to expression for LazyFrame
                return df.filter(pl.lit(mask))
            return df.filter(mask)
        return df.filter(mask)

    # ------------------------------------------------------------------
    # Iteration / conversion
    # ------------------------------------------------------------------

    def head(self, df: "pl.DataFrame | pl.LazyFrame", n: int = 5) -> "pl.DataFrame":
        # Always returns materialized DataFrame (collects LazyFrame if needed)
        if isinstance(df, pl.LazyFrame):
            return df.head(n).collect()
        return df.head(n)

    def itertuples(self, df: "pl.DataFrame | pl.LazyFrame", name: str) -> Any:
        """Iterate over rows, yielding named tuples."""
        materialized = df.collect() if isinstance(df, pl.LazyFrame) else df
        RowClass = namedtuple(name, ["Index"] + materialized.columns)  # type: ignore[misc]
        for i, row in enumerate(materialized.iter_rows(named=True)):
            yield RowClass(i, **row)  # type: ignore[call-arg]

    def equals(
        self, df1: "pl.DataFrame | pl.LazyFrame", df2: "pl.DataFrame | pl.LazyFrame"
    ) -> bool:
        d1 = df1.collect() if isinstance(df1, pl.LazyFrame) else df1
        d2 = df2.collect() if isinstance(df2, pl.LazyFrame) else df2
        return d1.equals(d2)

    def to_dict(self, df: "pl.DataFrame | pl.LazyFrame", orient: str = "records") -> Any:
        materialized = df.collect() if isinstance(df, pl.LazyFrame) else df
        if orient == "records":
            return materialized.to_dicts()
        elif orient == "dict" or orient == "list":
            return {col: materialized[col].to_list() for col in materialized.columns}
        else:
            return materialized.to_dicts()

    def to_csv(self, df: "pl.DataFrame | pl.LazyFrame", path: str, **kwargs: Any) -> None:
        materialized = df.collect() if isinstance(df, pl.LazyFrame) else df
        materialized.write_csv(path, **kwargs)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    def from_dict(self, data: Dict[str, list]) -> "pl.DataFrame":
        return pl.DataFrame(data)

    def from_records(self, records: List[dict]) -> "pl.DataFrame":
        return pl.DataFrame(records)

    def read_csv(self, path: str, **kwargs: Any) -> "pl.DataFrame":
        return pl.read_csv(path, **kwargs)  # type: ignore[no-any-return]

    def empty_series(self, dtype: str) -> "pl.Series":
        # Map string dtypes to polars dtypes
        dtype_map = {
            "int64": pl.Int64,
            "float64": pl.Float64,
            "str": pl.Utf8,
            "bool": pl.Boolean,
        }
        pl_dtype = dtype_map.get(dtype.lower(), pl.Utf8)
        return pl.Series("", [], dtype=pl_dtype)

    # ------------------------------------------------------------------
    # Pandera validation
    # ------------------------------------------------------------------

    def build_pandera_schema(
        self,
        sf_schema: Dict[str, dict],
        check_types: bool = True,
    ) -> Any:
        import pandera.polars as pa

        self._ensure_dtype_map()

        columns: Dict[str, pa.Column] = {}

        for attr_name, meta in sf_schema.items():
            df_col: str = meta["df_col"]
            inner_type = meta["inner_type"]
            fi = meta["field_info"]
            is_optional: bool = meta["is_optional"]

            checks: List[Any] = []

            # Numeric constraints
            if fi.ge is not None:
                checks.append(pa.Check.ge(fi.ge))
            if fi.gt is not None:
                checks.append(pa.Check.gt(fi.gt))
            if fi.le is not None:
                checks.append(pa.Check.le(fi.le))
            if fi.lt is not None:
                checks.append(pa.Check.lt(fi.lt))

            # Categorical constraint
            if fi.isin is not None:
                checks.append(pa.Check.isin(fi.isin))

            # String constraints
            if fi.regex is not None:
                checks.append(pa.Check.str_matches(fi.regex))
            if fi.min_length is not None or fi.max_length is not None:
                checks.append(
                    pa.Check.str_length(
                        min_value=fi.min_length,
                        max_value=fi.max_length,
                    )
                )

            # Determine Polars dtype for Pandera
            pa_dtype: Any = None
            if check_types and inner_type is not None:
                pa_dtype = self._POLARS_DTYPE_MAP.get(inner_type)

            columns[df_col] = pa.Column(
                dtype=pa_dtype,
                checks=checks or None,
                nullable=fi.nullable,
                unique=fi.unique,
                required=not is_optional,
                coerce=False,
            )

        return pa.DataFrameSchema(columns=columns)

    def validate_with_pandera(
        self,
        df: "pl.DataFrame | pl.LazyFrame",
        pandera_schema: Any,
        lazy: bool = True,
    ) -> None:
        import pandera.polars as pa

        # Pandera requires a materialised DataFrame for validation
        materialized = df.collect() if isinstance(df, pl.LazyFrame) else df

        try:
            pandera_schema.validate(materialized, lazy=lazy)
        except pa.errors.SchemaErrors as exc:
            self._translate_pandera_errors(exc)
        except pa.errors.SchemaError as exc:
            self._translate_single_pandera_error(exc)

    def _translate_pandera_errors(self, exc: Any) -> None:
        """Translate a Pandera SchemaErrors (lazy) into StructFrame exceptions."""
        fc = exc.failure_cases

        # Polars pandera returns a polars DataFrame for failure_cases
        if _HAS_POLARS and isinstance(fc, pl.DataFrame):
            checks = fc["check"].to_list()
            columns = fc["column"].to_list() if "column" in fc.columns else []

            # Check for missing columns
            for i, check in enumerate(checks):
                if check and "column_in_dataframe" in str(check):
                    col = fc["failure_case"][i] if "failure_case" in fc.columns else "?"
                    raise MissingColumnError(f"Missing required column: {col}") from exc

            # Check for dtype mismatches
            for i, check in enumerate(checks):
                if check and str(check).startswith("dtype("):
                    col = columns[i] if i < len(columns) else "?"
                    raise TypeMismatchError(f"Column '{col}' dtype mismatch: {check}") from exc

            # Everything else is a constraint violation
            if fc.height > 0:
                col = columns[0] if columns else "?"
                check = checks[0] if checks else "?"
                raise ConstraintViolationError(f"Column '{col}' failed check: {check}") from exc
        else:
            # Fallback: treat as pandas-style failure_cases
            self._translate_single_pandera_error(Exception(str(exc)))

    def _translate_single_pandera_error(self, exc: Any) -> None:
        """Translate a single Pandera SchemaError into a StructFrame exception."""
        msg = str(exc)
        if "not in dataframe" in msg or "column_in_dataframe" in msg:
            raise MissingColumnError(msg) from exc
        elif "dtype" in msg.lower():
            raise TypeMismatchError(msg) from exc
        else:
            raise ConstraintViolationError(msg) from exc

    # ------------------------------------------------------------------
    # Type coercion
    # ------------------------------------------------------------------

    def coerce_column(
        self,
        df: "pl.DataFrame",
        col: str,
        inner_type: Type,
        errors: str = "raise",
    ) -> "pl.DataFrame":
        self._ensure_dtype_map()
        target_dtype = self._POLARS_DTYPE_MAP.get(inner_type)

        if target_dtype is None:
            return df

        try:
            if inner_type == bool:
                # Handle string → bool conversion
                s = df[col]
                if s.dtype == pl.Utf8:
                    lower = s.str.to_lowercase()
                    bool_series = (
                        pl.when(lower.is_in(["true", "1", "yes", "on"]))
                        .then(True)
                        .when(lower.is_in(["false", "0", "no", "off"]))
                        .then(False)
                        .otherwise(None)
                        .alias(col)
                    )
                    df = df.with_columns(bool_series)
                else:
                    df = df.with_columns(df[col].cast(target_dtype).alias(col))
            elif errors == "coerce":
                # Polars uses strict=False for lenient casting
                df = df.with_columns(df[col].cast(target_dtype, strict=False).alias(col))
            else:
                df = df.with_columns(df[col].cast(target_dtype).alias(col))
        except Exception as e:
            if errors == "raise":
                raise TypeError(
                    f"Cannot coerce column '{col}' to {inner_type.__name__}: {e}"
                ) from e
        return df

    # ------------------------------------------------------------------
    # Example data generation
    # ------------------------------------------------------------------

    def generate_example_data(
        self,
        sf_schema: Dict[str, dict],
        nrows: int = 3,
    ) -> "pl.DataFrame":
        data: Dict[str, list] = {}
        for attr_name, meta in sf_schema.items():
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
                from datetime import timedelta

                base = datetime(2020, 1, 1)
                if inner_type == date:
                    data[col] = [(base + timedelta(days=i)).date() for i in range(nrows)]
                else:
                    data[col] = [(base + timedelta(days=i)) for i in range(nrows)]
            else:
                data[col] = [None] * nrows
        return pl.DataFrame(data)

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def schema_info_to_dataframe(self, rows: List[dict]) -> "pl.DataFrame":
        return pl.DataFrame(rows)

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def collect(self, df: "pl.DataFrame | pl.LazyFrame") -> "pl.DataFrame":
        if isinstance(df, pl.LazyFrame):
            return df.collect()
        return df
