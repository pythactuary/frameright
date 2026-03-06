"""Abstract base class for backend adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type


class BackendAdapter(ABC):
    """Abstract interface that every DataFrame backend must implement.

    StructFrame delegates all backend-specific operations (column access,
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
    # Index operations
    # ------------------------------------------------------------------

    @abstractmethod
    def get_index(self, df: Any) -> Any:
        """Return the index of the DataFrame."""
        ...

    @abstractmethod
    def set_index(self, df: Any, value: Any) -> Any:
        """Set the index. Returns the (possibly new) DataFrame."""
        ...

    @abstractmethod
    def get_index_level(self, df: Any, level_name: str) -> Any:
        """Return a single level of a MultiIndex by name."""
        ...

    @abstractmethod
    def set_index_level(self, df: Any, level_name: str, value: Any) -> Any:
        """Replace one level of a MultiIndex. Returns the (possibly new) DataFrame."""
        ...

    @abstractmethod
    def index_nlevels(self, df: Any) -> int:
        """Return the number of index levels."""
        ...

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    @abstractmethod
    def filter_rows(self, df: Any, mask: Any) -> Any:
        """Filter rows by a boolean mask. Returns a new DataFrame."""
        ...

    # ------------------------------------------------------------------
    # Iteration / conversion
    # ------------------------------------------------------------------

    @abstractmethod
    def head(self, df: Any, n: int = 5) -> Any:
        """Return the first *n* rows."""
        ...

    @abstractmethod
    def itertuples(self, df: Any, name: str) -> Any:
        """Iterate over rows, yielding named tuples (or equivalent)."""
        ...

    @abstractmethod
    def equals(self, df1: Any, df2: Any) -> bool:
        """Check data equality between two DataFrames."""
        ...

    @abstractmethod
    def to_dict(self, df: Any, orient: str = "records") -> Any:
        """Convert the DataFrame to a dictionary."""
        ...

    @abstractmethod
    def to_csv(self, df: Any, path: str, **kwargs: Any) -> None:
        """Write the DataFrame to a CSV file."""
        ...

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @abstractmethod
    def from_dict(self, data: Dict[str, list]) -> Any:
        """Create a DataFrame from a dictionary of lists."""
        ...

    @abstractmethod
    def from_records(self, records: List[dict]) -> Any:
        """Create a DataFrame from a list of row dictionaries."""
        ...

    @abstractmethod
    def read_csv(self, path: str, **kwargs: Any) -> Any:
        """Read a CSV file into a DataFrame."""
        ...

    @abstractmethod
    def empty_series(self, dtype: str) -> Any:
        """Create an empty Series with the given dtype string."""
        ...

    # ------------------------------------------------------------------
    # Pandera validation
    # ------------------------------------------------------------------

    @abstractmethod
    def build_pandera_schema(
        self,
        sf_schema: Dict[str, dict],
        check_types: bool = True,
    ) -> Any:
        """Build a Pandera DataFrameSchema from the parsed StructFrame schema.

        Args:
            sf_schema: The ``_sf_schema`` dict from a StructFrame subclass.
            check_types: Whether to include dtype checks.

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
        """Run Pandera validation and translate errors into StructFrame exceptions.

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
    ) -> Any:
        """Coerce a single column to match *inner_type*. Returns the (possibly new) DataFrame."""
        ...

    # ------------------------------------------------------------------
    # Example data generation
    # ------------------------------------------------------------------

    @abstractmethod
    def generate_example_data(
        self,
        sf_schema: Dict[str, dict],
        nrows: int = 3,
    ) -> Any:
        """Generate a DataFrame with dummy data matching the schema."""
        ...

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    @abstractmethod
    def schema_info_to_dataframe(self, rows: List[dict]) -> Any:
        """Convert a list of schema-info dicts into a backend-native DataFrame."""
        ...
