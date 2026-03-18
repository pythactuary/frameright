"""Narwhals backend adapter for Schema.

This backend handles narwhals DataFrames (nw.DataFrame) for users who want
backend-agnostic code. For native pandas or polars functionality, use the
PandasBackend or PolarsBackend respectively.

Users would use this by wrapping their DataFrame:
    import narwhals as nw
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3]})
    nw_df = nw.from_native(df)

    class MyFrame(Schema):
        a: Col[int]

    frame = MyFrame(nw_df)  # Uses NarwhalsBackend
    frame.a  # Returns nw.Series
"""

from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime
from typing import Any, Dict, List

import narwhals as nw

from ..exceptions import ConstraintViolationError, MissingColumnError, TypeMismatchError
from .base import BackendAdapter


class NarwhalsBackend(BackendAdapter):
    """Backend adapter for narwhals DataFrames (backend-agnostic operations)."""

    @property
    def name(self) -> str:
        return "narwhals"

    # DataFrame operations
    def copy(self, df: nw.DataFrame) -> nw.DataFrame:
        return df.clone() if hasattr(df, "clone") else df

    def get_column(self, df: nw.DataFrame, col: str) -> nw.Series:
        return df[col]

    def get_column_ref(self, df: nw.DataFrame, col: str) -> nw.Series:
        return df[col]

    def set_column(self, df: nw.DataFrame, col: str, value: Any) -> nw.DataFrame:
        if isinstance(value, nw.Series):
            return df.with_columns(value.alias(col))
        # Let narwhals handle scalars/arrays
        return df.with_columns(**{col: value})

    def has_column(self, df: nw.DataFrame, col: str) -> bool:
        # For LazyFrames, use collect_schema().names() to avoid performance warning
        if hasattr(df, "collect_schema"):
            return col in df.collect_schema().names()
        return col in df.columns

    def column_names(self, df: nw.DataFrame) -> List[str]:
        return list(df.columns)

    def num_rows(self, df: nw.DataFrame) -> int:
        return len(df)

    def num_cols(self, df: nw.DataFrame) -> int:
        return len(df.columns)

    # Index operations (narwhals doesn't have indices)
    def get_index(self, df: nw.DataFrame) -> List[int]:
        return list(range(len(df)))

    def set_index(self, df: nw.DataFrame, value: Any) -> nw.DataFrame:
        return df  # No-op for narwhals

    def get_index_level(self, df: nw.DataFrame, level_name: str) -> nw.Series:
        if level_name in df.columns:
            return df[level_name]
        raise KeyError(f"No column '{level_name}' found")

    def set_index_level(
        self, df: nw.DataFrame, level_name: str, value: Any
    ) -> nw.DataFrame:
        return self.set_column(df, level_name, value)

    def index_nlevels(self, df: nw.DataFrame) -> int:
        return 1

    # Filtering
    def filter_rows(self, df: nw.DataFrame, mask: nw.Series) -> nw.DataFrame:
        return df.filter(mask)

    # Iteration / conversion
    def head(self, df: nw.DataFrame, n: int = 5) -> nw.DataFrame:
        return df.head(n)

    def itertuples(self, df: nw.DataFrame, name: str) -> Any:
        # Convert to native for iteration
        native_df = df.to_native()
        if hasattr(native_df, "itertuples"):  # Pandas
            return native_df.itertuples(index=True, name=name)
        else:  # Polars
            RowClass = namedtuple(name, ["Index"] + list(df.columns))
            for i, row in enumerate(native_df.iter_rows(named=True)):
                yield RowClass(i, **row)

    def equals(self, df1: nw.DataFrame, df2: nw.DataFrame) -> bool:
        # Compare via native
        return df1.to_native().equals(df2.to_native())

    def to_dict(self, df: nw.DataFrame, orient: str = "records") -> Any:
        # Convert to dict via native
        native = df.to_native()
        if hasattr(native, "to_dict"):  # Pandas
            return native.to_dict(orient=orient)
        else:  # Polars
            if orient == "records":
                return native.to_dicts()
            return {col: native[col].to_list() for col in native.columns}

    def to_csv(self, df: nw.DataFrame, path: str, **kwargs: Any) -> None:
        native = df.to_native()
        if hasattr(native, "to_csv"):  # Pandas
            native.to_csv(path, index=False, **kwargs)
        else:  # Polars
            native.write_csv(path, **kwargs)

    # Construction helpers (return native for narwhals to wrap)
    def read_csv(self, path: str, **kwargs: Any) -> Any:
        import pandas as pd

        return nw.from_native(pd.read_csv(path, **kwargs))

    def empty_series(self, dtype: str) -> nw.Series:
        import pandas as pd

        return nw.from_native(pd.Series([], dtype=dtype), series_only=True)

    # Pandera validation (narwhals wraps native, so validate the native)
    def build_pandera_schema(
        self,
        fr_schema: Dict[str, dict],
        df: nw.DataFrame,
        check_types: bool = True,
        strict: bool = False,
    ) -> Any:
        """Build pandera schema based on the underlying native DataFrame."""
        native = df.to_native()
        is_polars = (
            hasattr(native, "__class__") and "polars" in native.__class__.__module__
        )

        if is_polars:
            import pandera.polars as pa
            import polars as pl

            dtype_map: Dict[type, Any] = {
                int: pl.Int64,
                float: pl.Float64,
                str: pl.String,
                bool: pl.Boolean,
                datetime: pl.Datetime,
                date: pl.Date,
            }
        else:
            import pandera.pandas as pa

            dtype_map: Dict[type, Any] = {
                int: int,
                float: float,
                str: str,
                bool: bool,
                datetime: "datetime64[ns]",
                date: "datetime64[ns]",
            }

        columns: Dict[str, pa.Column] = {}

        for attr_name, meta in fr_schema.items():
            df_col: str = meta["df_col"]
            inner_type = meta["inner_type"]
            fi = meta["field_info"]
            is_optional: bool = meta["is_optional"]

            checks: List[Any] = []

            if fi.ge is not None:
                checks.append(pa.Check.ge(fi.ge))
            if fi.gt is not None:
                checks.append(pa.Check.gt(fi.gt))
            if fi.le is not None:
                checks.append(pa.Check.le(fi.le))
            if fi.lt is not None:
                checks.append(pa.Check.lt(fi.lt))
            if fi.isin is not None:
                checks.append(pa.Check.isin(fi.isin))
            if fi.regex is not None:
                checks.append(pa.Check.str_matches(fi.regex))
            if fi.min_length is not None or fi.max_length is not None:
                checks.append(
                    pa.Check.str_length(
                        min_value=fi.min_length,
                        max_value=fi.max_length,
                    )
                )

            pa_dtype: Any = None
            if check_types and inner_type is not None:
                pa_dtype = dtype_map.get(inner_type)
                if not is_polars and inner_type is bool and fi.nullable:
                    pa_dtype = "boolean"

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
        df: nw.DataFrame,
        pandera_schema: Any,
        lazy: bool = True,
    ) -> None:
        from pandera import errors

        native = df.to_native()

        try:
            pandera_schema.validate(native, lazy=lazy)
        except errors.SchemaErrors as exc:
            self._translate_pandera_errors(exc)
        except errors.SchemaError as exc:
            self._translate_single_pandera_error(exc)

    def _translate_pandera_errors(self, exc: Any) -> None:
        fc = exc.failure_cases
        if hasattr(fc, "to_pandas"):
            fc = fc.to_pandas()

        missing_mask = fc["check"] == "column_in_dataframe"
        if missing_mask.any():
            missing_cols = sorted(
                fc.loc[missing_mask, "failure_case"].unique().tolist()
            )
            raise MissingColumnError(
                f"Missing required columns: {missing_cols}"
            ) from exc

        dtype_mask = fc["check"].str.startswith("dtype(", na=False)
        if dtype_mask.any():
            row = fc.loc[dtype_mask].iloc[0]
            raise TypeMismatchError(
                f"Column '{row['column']}' dtype mismatch: {row['check']}"
            ) from exc

        if len(fc) > 0:
            row = fc.iloc[0]
            col = row.get("column", "?")
            check = row.get("check", "?")
            raise ConstraintViolationError(
                f"Column '{col}' failed check: {check}"
            ) from exc

    def _translate_single_pandera_error(self, exc: Any) -> None:
        msg = str(exc)
        if "not in dataframe" in msg or "column_in_dataframe" in msg:
            raise MissingColumnError(msg) from exc
        elif "dtype" in msg.lower():
            raise TypeMismatchError(msg) from exc
        else:
            raise ConstraintViolationError(msg) from exc

    # Type coercion
    def coerce_column(
        self,
        df: nw.DataFrame,
        col: str,
        target_type: type,
    ) -> nw.DataFrame:
        """Coerce a column to target type using narwhals."""
        # Narwhals dtype mapping
        dtype_map = {
            int: nw.Int64,
            float: nw.Float64,
            str: nw.String,
            bool: nw.Boolean,
        }

        target_dtype = dtype_map.get(target_type)
        if target_dtype is None:
            return df

        return df.with_columns(df[col].cast(target_dtype).alias(col))

    # Lazy evaluation
    def is_lazy(self, df: Any) -> bool:
        # Narwhals DataFrames are eager
        return False

    def collect(self, df: nw.DataFrame) -> nw.DataFrame:
        # Narwhals DataFrames are already collected
        return df

    def schema_info_to_dataframe(self, rows: List[dict]) -> nw.DataFrame:
        """Convert schema info rows to narwhals DataFrame."""
        import pandas as pd

        pd_df = pd.DataFrame(rows)
        return nw.from_native(pd_df)
