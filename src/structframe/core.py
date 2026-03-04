from __future__ import annotations
import pandas as pd
from typing import Any, Dict, Type, get_type_hints, get_origin, get_args, Union, TypeVar

TStructFrame = TypeVar("TStructFrame", bound="StructFrame")


class StructFrame:
    """
    Base class for Object-DataFrame Mapping.
    Automatically generates properties, schemas, and validation from type hints.
    """

    # Class-level sets to store schema requirements
    _required_cols: set
    _optional_cols: set

    def __init__(self, df: pd.DataFrame, copy: bool = False, validate: bool = True):
        """
        Initializes the model.
        - copy: Defaults to False to save memory on large DataFrames.
        - validate: Defaults to True to strictly enforce the contract immediately.
        """
        self._df = df.copy() if copy else df

        if validate:
            self.validate()

    def __init_subclass__(cls, **kwargs):
        """Metaclass hook to build properties and schema definitions at import time."""
        super().__init_subclass__(**kwargs)

        cls._required_cols = set()
        cls._optional_cols = set()

        hints = get_type_hints(cls)
        for attr_name, attr_type in hints.items():
            if attr_name.startswith("_"):
                continue

            # HIGH VALUE: Support for Optional[] columns
            # Optional[X] is evaluated by Python as Union[X, NoneType]
            origin = get_origin(attr_type)
            args = get_args(attr_type)
            is_optional = origin is Union and type(None) in args

            if is_optional:
                cls._optional_cols.add(attr_name)
            else:
                cls._required_cols.add(attr_name)

            # Property Factory
            def make_property(col_name: str):
                def getter(self) -> pd.Series:
                    return self._df[col_name]

                def setter(self, value: Any):
                    self._df[col_name] = value

                return property(getter, setter)

            setattr(cls, attr_name, make_property(attr_name))

    # --- Validation ---
    def validate(self) -> "StructFrame":
        """Checks if all required columns defined in the schema exist."""
        actual_cols = set(self._df.columns)
        missing_required = self._required_cols - actual_cols

        if missing_required:
            raise ValueError(
                f"[{self.__class__.__name__} Validation Error] "
                f"Missing required columns: {sorted(list(missing_required))}"
            )
        return self

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        return {k: v for k, v in get_type_hints(cls).items() if not k.startswith("_")}

    # --- Pandas Escape Hatches & Interoperability ---
    @property
    def data(self) -> pd.DataFrame:
        """Escape hatch to get the raw pandas dataframe."""
        return self._df

    @property
    def index(self) -> pd.Index:
        """EASY WIN: Expose the DataFrame index directly."""
        return self._df.index

    # CRITICAL FIX: Use TypeVar (TStructFrame) so IDEs know this returns the exact child class
    def filter(self: TStructFrame, condition: pd.Series) -> TStructFrame:
        """Filters the dataframe and returns a NEW instance of the structured object."""
        # Note: validate=False because filtering drops rows, not columns, so schema is safe.
        return self.__class__(self._df[condition], copy=False, validate=False)

    # --- Python Data Model Protocols (EASY WINS) ---
    def __len__(self) -> int:
        """Allows calling len(portfolio) to get the row count."""
        return len(self._df)

    def __repr__(self) -> str:
        """HIGH VALUE: Beautiful debugging output instead of memory addresses."""
        req = len(self._required_cols)
        opt = len(self._optional_cols)
        return (
            f"<{self.__class__.__name__}: {len(self)} rows x {len(self._df.columns)} cols "
            f"({req} required, {opt} optional fields)>\n"
            f"{self._df.head()}"
        )
