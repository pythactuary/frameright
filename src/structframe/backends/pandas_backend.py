"""Pandas backend adapter for StructFrame."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import pandas.api.types as ptypes

from ..exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    TypeMismatchError,
)
from .base import BackendAdapter


class PandasBackend(BackendAdapter):
    """Backend adapter for Pandas DataFrames."""

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "pandas"

    # ------------------------------------------------------------------
    # Dtype mapping (Python type ➜ Pandera/Pandas dtype)
    # ------------------------------------------------------------------

    _PANDERA_DTYPE_MAP: Dict[type, Any] = {
        int: int,
        float: float,
        str: str,
        bool: bool,
        datetime: "datetime64[ns]",
        date: "datetime64[ns]",
    }

    # ------------------------------------------------------------------
    # DataFrame operations
    # ------------------------------------------------------------------

    def copy(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.copy()

    def get_column(self, df: pd.DataFrame, col: str) -> pd.Series:
        return df[col]

    def set_column(self, df: pd.DataFrame, col: str, value: Any) -> pd.DataFrame:
        df[col] = value
        return df

    def has_column(self, df: pd.DataFrame, col: str) -> bool:
        return col in df.columns

    def column_names(self, df: pd.DataFrame) -> List[str]:
        return list(df.columns)

    def num_rows(self, df: pd.DataFrame) -> int:
        return len(df)

    def num_cols(self, df: pd.DataFrame) -> int:
        return len(df.columns)

    # ------------------------------------------------------------------
    # Index operations
    # ------------------------------------------------------------------

    def get_index(self, df: pd.DataFrame) -> pd.Index:
        return df.index

    def set_index(self, df: pd.DataFrame, value: Any) -> pd.DataFrame:
        df.index = value
        return df

    def get_index_level(self, df: pd.DataFrame, level_name: str) -> pd.Index:
        return df.index.get_level_values(level_name)

    def set_index_level(self, df: pd.DataFrame, level_name: str, value: Any) -> pd.DataFrame:
        idx = df.index
        arrays = [
            value if idx.names[i] == level_name else idx.get_level_values(i)
            for i in range(idx.nlevels)
        ]
        df.index = pd.MultiIndex.from_arrays(arrays, names=idx.names)
        return df

    def index_nlevels(self, df: pd.DataFrame) -> int:
        return df.index.nlevels

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_rows(self, df: pd.DataFrame, mask: Any) -> pd.DataFrame:
        return df[mask]

    # ------------------------------------------------------------------
    # Iteration / conversion
    # ------------------------------------------------------------------

    def head(self, df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        return df.head(n)

    def itertuples(self, df: pd.DataFrame, name: str) -> Any:
        return df.itertuples(index=True, name=name)

    def equals(self, df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        return df1.equals(df2)

    def to_dict(self, df: pd.DataFrame, orient: str = "records") -> Any:
        return df.to_dict(orient=orient)

    def to_csv(self, df: pd.DataFrame, path: str, **kwargs: Any) -> None:
        df.to_csv(path, index=False, **kwargs)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    def from_dict(self, data: Dict[str, list]) -> pd.DataFrame:
        return pd.DataFrame(data)

    def from_records(self, records: List[dict]) -> pd.DataFrame:
        return pd.DataFrame.from_records(records)

    def read_csv(self, path: str, **kwargs: Any) -> pd.DataFrame:
        return pd.read_csv(path, **kwargs)

    def empty_series(self, dtype: str) -> pd.Series:
        return pd.Series([], dtype=dtype)

    # ------------------------------------------------------------------
    # Pandera validation
    # ------------------------------------------------------------------

    def build_pandera_schema(
        self,
        sf_schema: Dict[str, dict],
        check_types: bool = True,
    ) -> Any:
        import pandera.pandas as pa

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

            # Determine dtype for Pandera
            pa_dtype: Any = None
            if check_types and inner_type is not None:
                pa_dtype = self._PANDERA_DTYPE_MAP.get(inner_type)

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
        df: pd.DataFrame,
        pandera_schema: Any,
        lazy: bool = True,
    ) -> None:
        import pandera.pandas as pa

        try:
            pandera_schema.validate(df, lazy=lazy)
        except pa.errors.SchemaErrors as exc:
            self._translate_pandera_errors(exc)
        except pa.errors.SchemaError as exc:
            self._translate_single_pandera_error(exc)

    def _translate_pandera_errors(self, exc: Any) -> None:
        """Translate a Pandera SchemaErrors (lazy) into StructFrame exceptions."""
        fc = exc.failure_cases

        # Check for missing columns first
        missing_mask = fc["check"] == "column_in_dataframe"
        if missing_mask.any():
            missing_cols = sorted(fc.loc[missing_mask, "failure_case"].unique().tolist())
            raise MissingColumnError(f"Missing required columns: {missing_cols}") from exc

        # Check for dtype mismatches
        dtype_mask = fc["check"].str.startswith("dtype(", na=False)
        if dtype_mask.any():
            row = fc.loc[dtype_mask].iloc[0]
            raise TypeMismatchError(
                f"Column '{row['column']}' dtype mismatch: {row['check']}"
            ) from exc

        # Everything else is a constraint violation
        if len(fc) > 0:
            row = fc.iloc[0]
            col = row.get("column", "?")
            check = row.get("check", "?")
            raise ConstraintViolationError(f"Column '{col}' failed check: {check}") from exc

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
        df: pd.DataFrame,
        col: str,
        inner_type: Type,
        errors: str = "raise",
    ) -> pd.DataFrame:
        try:
            if inner_type == int:
                df[col] = pd.to_numeric(df[col], errors=errors)
                try:
                    df[col] = df[col].astype("Int64")
                except (TypeError, ValueError):
                    pass
            elif inner_type == float:
                df[col] = pd.to_numeric(df[col], errors=errors)
            elif inner_type == str:
                df[col] = df[col].astype(str)
            elif inner_type == bool:
                if ptypes.is_object_dtype(df[col]) or ptypes.is_string_dtype(df[col]):
                    df[col] = df[col].astype(object)
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
    ) -> pd.DataFrame:
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
                data[col] = pd.date_range("2020-01-01", periods=nrows, freq="D").tolist()
            else:
                data[col] = [None] * nrows
        return pd.DataFrame(data)

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def schema_info_to_dataframe(self, rows: List[dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)
