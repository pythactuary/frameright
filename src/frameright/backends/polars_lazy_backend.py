"""Polars lazy (LazyFrame) backend adapter for Schema."""

from __future__ import annotations

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


class PolarsLazyBackend(BackendAdapter):
    """Backend adapter for Polars LazyFrames (lazy evaluation)."""

    def __init__(self) -> None:
        _require_polars()

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "polars_lazy"

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

    def copy(self, df: "pl.LazyFrame") -> "pl.LazyFrame":
        return df  # LazyFrames are immutable query plans; no copy needed

    def get_column(self, df: "pl.LazyFrame", col: str) -> "pl.Expr":
        # For LazyFrame, we return an expression, not a materialized Series
        raise TypeError(
            "Cannot materialize a column from a LazyFrame. "
            "Use get_column_ref() to obtain a pl.col() expression instead."
        )

    def get_column_ref(self, df: Any, col: str) -> "pl.Expr":
        """Return a lazy ``pl.col()`` expression — preserves the query optimizer."""
        return pl.col(col)

    def set_column(self, df: "pl.LazyFrame", col: str, value: Any) -> "pl.LazyFrame":
        if isinstance(value, pl.Expr):
            new_col = value.alias(col)
        elif isinstance(value, pl.Series):
            # Convert Series to expression for LazyFrame
            new_col = pl.lit(value).alias(col)
        else:
            new_col = pl.lit(value).alias(col)
        return df.with_columns(new_col)

    def has_column(self, df: "pl.LazyFrame", col: str) -> bool:
        return col in df.collect_schema().names()

    def column_names(self, df: "pl.LazyFrame") -> List[str]:
        return df.collect_schema().names()

    def num_rows(self, df: "pl.LazyFrame") -> int:
        return df.collect().height

    def num_cols(self, df: "pl.LazyFrame") -> int:
        return df.collect_schema().len()

    # ------------------------------------------------------------------
    # Iteration / conversion
    # ------------------------------------------------------------------

    def head(self, df: "pl.LazyFrame", n: int = 5) -> "pl.DataFrame":
        # Always returns materialized DataFrame (collects LazyFrame)
        return df.head(n).collect()

    def equals(self, df1: "pl.LazyFrame", df2: "pl.LazyFrame") -> bool:
        d1 = df1.collect()
        d2 = df2.collect()
        return d1.equals(d2)

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
        df: "pl.LazyFrame",
        pandera_schema: Any,
        lazy: bool = True,
    ) -> None:
        import pandera.polars as pa

        # Pandera requires a materialised DataFrame for validation
        materialized = df.collect()

        try:
            pandera_schema.validate(materialized, lazy=lazy)
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
        df: "pl.LazyFrame",
        col: str,
        inner_type: Type,
        errors: str = "raise",
        nullable: bool = True,
    ) -> "pl.LazyFrame":
        self._ensure_dtype_map()
        target_dtype = self._POLARS_DTYPE_MAP.get(inner_type)

        if target_dtype is None:
            return df

        try:
            if inner_type is bool:
                # Handle string → bool conversion
                lower = pl.col(col).str.to_lowercase()
                bool_col = (
                    pl.when(lower.is_in(["true", "1", "yes", "on"]))
                    .then(True)
                    .when(lower.is_in(["false", "0", "no", "off"]))
                    .then(False)
                    .otherwise(None)
                    .alias(col)
                )
                df = df.with_columns(bool_col)
            elif errors == "coerce":
                # Polars uses strict=False for lenient casting
                df = df.with_columns(
                    pl.col(col).cast(target_dtype, strict=False).alias(col)
                )
            else:
                df = df.with_columns(pl.col(col).cast(target_dtype).alias(col))
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

    def collect(self, df: "pl.LazyFrame") -> "pl.DataFrame":
        return df.collect()
