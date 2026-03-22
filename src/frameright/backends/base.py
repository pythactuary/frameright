"""Abstract base class for backend adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type


class BackendAdapter(ABC):
    """Abstract interface that every DataFrame backend must implement.

    Schema delegates all backend-specific operations (column access,
    dtype inspection, validation, coercion, etc.) to a concrete adapter.
    """

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """Short name for the backend, e.g. 'pandas' or 'polars'."""
        ...

    # ------------------------------------------------------------------
    # DataFrame operations
    # ------------------------------------------------------------------

    @abstractmethod
    def copy(self, df: Any) -> Any:
        """Return a deep copy of the DataFrame."""
        ...

    @abstractmethod
    def get_column(self, df: Any, col: str) -> Any:
        """Return a column (Series) from the DataFrame."""
        ...

    @abstractmethod
    def get_column_ref(self, df: Any, col: str) -> Any:
        """Return a *lazy* column reference for use in property getters.

        For eager backends (Pandas) this is the same as ``get_column``
        and returns the materialised ``pd.Series``.

        For lazy-capable backends (Polars) this returns ``pl.col(col)``
        — a lazy expression that preserves the query optimizer.
        """
        ...

    @abstractmethod
    def set_column(self, df: Any, col: str, value: Any) -> Any:
        """Set/replace a column. Returns the (possibly new) DataFrame.

        For mutable backends (Pandas) the original may be mutated and returned.
        For immutable backends (Polars) a new DataFrame is returned.
        """
        ...

    @abstractmethod
    def has_column(self, df: Any, col: str) -> bool:
        """Check whether *col* exists as a column name."""
        ...

    @abstractmethod
    def column_names(self, df: Any) -> List[str]:
        """Return the list of column names."""
        ...

    @abstractmethod
    def num_rows(self, df: Any) -> int:
        """Return the number of rows."""
        ...

    @abstractmethod
    def num_cols(self, df: Any) -> int:
        """Return the number of columns."""
        ...

    # ------------------------------------------------------------------
    # Iteration / conversion
    # ------------------------------------------------------------------

    @abstractmethod
    def head(self, df: Any, n: int = 5) -> Any:
        """Return the first *n* rows."""
        ...

    @abstractmethod
    def equals(self, df1: Any, df2: Any) -> bool:
        """Check data equality between two DataFrames."""
        ...

    # ------------------------------------------------------------------
    # Pandera validation
    # ------------------------------------------------------------------

    @abstractmethod
    def build_pandera_schema(
        self,
        fr_schema: Dict[str, dict],
        df: Optional[Any] = None,
        check_types: bool = True,
        strict: bool = False,
    ) -> Any:
        """Build a Pandera DataFrameSchema from the parsed Schema schema.

        Args:
            fr_schema: The ``_fr_schema`` dict from a Schema subclass.
            df: Optional native dataframe (used by narwhals to detect backend type).
            check_types: Whether to include dtype checks.
            strict: If True, reject DataFrames with columns not in the schema.

        Returns:
            A ``pandera.DataFrameSchema`` instance for this backend.
        """
        ...

    @abstractmethod
    def validate_with_pandera(
        self,
        df: Any,
        pandera_schema: Any,
        lazy: bool = True,
    ) -> None:
        """Run Pandera validation and translate errors into Schema exceptions.

        Args:
            df: The DataFrame to validate.
            pandera_schema: The Pandera schema to validate against.
            lazy: If True, collect all errors before raising.

        Raises:
            MissingColumnError, TypeMismatchError, ConstraintViolationError
        """
        ...

    # ------------------------------------------------------------------
    # Type coercion
    # ------------------------------------------------------------------

    @abstractmethod
    def coerce_column(
        self,
        df: Any,
        col: str,
        inner_type: Type,
        errors: str = "raise",
        nullable: bool = True,
    ) -> Any:
        """Coerce a single column to match *inner_type*. Returns the (possibly new) DataFrame."""
        ...

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    @abstractmethod
    def schema_info_to_dataframe(self, rows: List[dict]) -> Any:
        """Convert a list of schema-info dicts into a backend-native DataFrame."""
        ...

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    @abstractmethod
    def collect(self, df: Any) -> Any:
        """Materialise a lazy frame into an eager one.

        For eager backends (Pandas, Polars DataFrame) this should return
        *df* unchanged.  For lazy backends (Polars LazyFrame) this should
        call ``df.collect()``.
        """
        ...
