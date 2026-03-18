"""Polars eager (DataFrame) backend adapter for Schema."""

from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type

from ..exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    TypeMismatchError,
    ValidationError,
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
            "Install it with: pip install frameright[polars]"
        )


class PolarsEagerBackend(BackendAdapter):
    """Backend adapter for Polars DataFrames (eager evaluation)."""

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

    def copy(self, df: "pl.DataFrame") -> "pl.DataFrame":
        return df.clone()

    def get_column(self, df: "pl.DataFrame", col: str) -> "pl.Series":
        return df[col]

    def get_column_ref(self, df: Any, col: str) -> "pl.Expr":
        """Return a lazy ``pl.col()`` expression — preserves the query optimizer."""
        return pl.col(col)

    def set_column(self, df: "pl.DataFrame", col: str, value: Any) -> "pl.DataFrame":
        if isinstance(value, pl.Expr):
            new_col = value.alias(col)
        elif isinstance(value, pl.Series):
            new_col = value.alias(col)  # type: ignore[assignment]
        else:
            new_col = pl.lit(value).alias(col)
        return df.with_columns(new_col)

    def has_column(self, df: "pl.DataFrame", col: str) -> bool:
        return col in df.columns

    def column_names(self, df: "pl.DataFrame") -> List[str]:
        return df.columns

    def num_rows(self, df: "pl.DataFrame") -> int:
        return df.height

    def num_cols(self, df: "pl.DataFrame") -> int:
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

    def set_index_level(
        self, df: "pl.DataFrame", level_name: str, value: Any
    ) -> "pl.DataFrame":
        return self.set_column(df, level_name, value)  # type: ignore[return-value]

    def index_nlevels(self, df: "pl.DataFrame") -> int:
        # Polars doesn't support multi-level indices
        return 1

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_rows(self, df: "pl.DataFrame", mask: Any) -> "pl.DataFrame":
        if isinstance(mask, pl.Expr):
            return df.filter(mask)
        if isinstance(mask, pl.Series):
            return df.filter(mask)
        return df.filter(mask)

    # ------------------------------------------------------------------
    # Iteration / conversion
    # ------------------------------------------------------------------

    def head(self, df: "pl.DataFrame", n: int = 5) -> "pl.DataFrame":
        return df.head(n)

    def itertuples(self, df: "pl.DataFrame", name: str) -> Any:
        """Iterate over rows, yielding named tuples."""
        RowClass = namedtuple(name, ["Index"] + df.columns)  # type: ignore[misc]
        for i, row in enumerate(df.iter_rows(named=True)):
            yield RowClass(i, **row)  # type: ignore[call-arg]

    def equals(self, df1: "pl.DataFrame", df2: "pl.DataFrame") -> bool:
        return df1.equals(df2)

    def to_dict(self, df: "pl.DataFrame", orient: str = "records") -> Any:
        if orient == "records":
            return df.to_dicts()
        elif orient == "dict" or orient == "list":
            return {col: df[col].to_list() for col in df.columns}
        else:
            return df.to_dicts()

    def to_csv(self, df: "pl.DataFrame", path: str, **kwargs: Any) -> None:
        df.write_csv(path, **kwargs)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

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
        fr_schema: Dict[str, dict],
        df: Optional[Any] = None,
        check_types: bool = True,
        strict: bool = False,
    ) -> Any:
        import pandera.polars as pa

        self._ensure_dtype_map()

        columns: Dict[str, pa.Column] = {}

        for attr_name, meta in fr_schema.items():
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

        return pa.DataFrameSchema(columns=columns, strict=strict)

    def validate_with_pandera(
        self,
        df: "pl.DataFrame",
        pandera_schema: Any,
        lazy: bool = True,
    ) -> None:
        import pandera.polars as pa

        try:
            pandera_schema.validate(df, lazy=lazy)
        except pa.errors.SchemaErrors as exc:
            self._translate_pandera_errors(exc)
        except pa.errors.SchemaError as exc:
            self._translate_single_pandera_error(exc)

    def _translate_pandera_errors(self, exc: Any) -> None:
        """Translate a Pandera SchemaErrors (lazy) into Schema exceptions."""
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

            # Check for extra columns (strict mode)
            for i, check in enumerate(checks):
                if check and "column_in_schema" in str(check):
                    # Get column name from error field if available
                    if "error" in fc.columns:
                        error_msg = str(fc["error"][i])
                        import re

                        match = re.search(r"column '([^']+)' not in", error_msg)
                        if match:
                            col_name = match.group(1)
                            raise ValidationError(
                                f"Column '{col_name}' is not defined in the schema (strict mode)"
                            ) from exc
                    raise ValidationError(
                        "Extra columns not allowed in strict mode"
                    ) from exc

            # Check for dtype mismatches
            for i, check in enumerate(checks):
                if check and str(check).startswith("dtype("):
                    col = columns[i] if i < len(columns) else "?"
                    raise TypeMismatchError(
                        f"Column '{col}' dtype mismatch: {check}"
                    ) from exc

            # Everything else is a constraint violation
            if fc.height > 0:
                col = columns[0] if columns else "?"
                check = checks[0] if checks else "?"
                raise ConstraintViolationError(
                    f"Column '{col}' failed check: {check}"
                ) from exc
        else:
            # Fallback: treat as pandas-style failure_cases
            self._translate_single_pandera_error(Exception(str(exc)))

    def _translate_single_pandera_error(self, exc: Any) -> None:
        """Translate a single Pandera SchemaError into a Schema exception."""
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
        nullable: bool = True,
    ) -> "pl.DataFrame":
        self._ensure_dtype_map()
        target_dtype = self._POLARS_DTYPE_MAP.get(inner_type)

        if target_dtype is None:
            return df

        try:
            if inner_type is bool:
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
                df = df.with_columns(
                    df[col].cast(target_dtype, strict=False).alias(col)
                )
            else:
                df = df.with_columns(df[col].cast(target_dtype).alias(col))
        except Exception as e:
            if errors == "raise":
                raise TypeError(
                    f"Cannot coerce column '{col}' to {inner_type.__name__}: {e}"
                ) from e
        return df

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def schema_info_to_dataframe(self, rows: List[dict]) -> "pl.DataFrame":
        return pl.DataFrame(rows)

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def collect(self, df: "pl.DataFrame") -> "pl.DataFrame":
        return df
