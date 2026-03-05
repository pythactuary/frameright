import pandas as pd
import pandas.api.types as ptypes
from typing import (
    Any,
    Dict,
    get_type_hints,
    get_origin,
    get_args,
    Union,
    TypeVar,
    Optional,
)
from .typing import Col

TStructFrame = TypeVar("TStructFrame", bound="StructFrame")


class FieldInfo:
    """Stores metadata for column mapping and field-level validation."""

    def __init__(
        self,
        alias: Optional[str] = None,
        ge: Optional[float] = None,
        le: Optional[float] = None,
        isin: Optional[list] = None,
    ):
        self.alias = alias
        self.ge = ge
        self.le = le
        self.isin = isin


def Field(
    alias: str = None, ge: float = None, le: float = None, isin: list = None
) -> Any:
    """Helper function to define a field's properties and constraints."""
    return FieldInfo(alias=alias, ge=ge, le=le, isin=isin)


class StructFrame:
    """Base class for the Object-DataFrame Mapper (ODM)."""

    # Stores the parsed schema for the specific child class
    _sf_schema: Dict[str, dict]

    def __init__(self, df: pd.DataFrame, copy: bool = False, validate: bool = True):
        # Prefix internal variables to avoid user column collisions
        self._sf_df = df.copy() if copy else df

        if validate:
            self.sf_validate()

    def __init_subclass__(cls, **kwargs):
        """Metaclass hook to parse the schema and inject properties at load time."""
        super().__init_subclass__(**kwargs)
        cls._sf_schema = {}

        hints = get_type_hints(cls)
        for attr_name, attr_type in hints.items():
            if attr_name.startswith("_"):
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

            # 2. Parse Field Metadata (Alias and Validation constraints)
            class_var = getattr(cls, attr_name, None)
            if isinstance(class_var, FieldInfo):
                field_info = class_var
                actual_df_col = field_info.alias or attr_name
            else:
                field_info = FieldInfo()
                actual_df_col = attr_name

            # Store the parsed schema for validation later
            cls._sf_schema[attr_name] = {
                "df_col": actual_df_col,
                "inner_type": inner_type,
                "field_info": field_info,
                "is_optional": is_optional,
            }

            # 3. Inject the safe Property wrapper
            # We now pass `is_optional` into the factory so the getter knows how to behave
            def make_property(col_name: str, optional_flag: bool):
                def getter(self):
                    # If it's optional and missing, return None safely
                    if optional_flag and col_name not in self._sf_df.columns:
                        return None
                    return self._sf_df[col_name]

                def setter(self, value: Any):
                    self._sf_df[col_name] = value

                return property(getter, setter)

            # Pass the is_optional boolean we calculated earlier
            setattr(cls, attr_name, make_property(actual_df_col, is_optional))

    # --- Core Methods (Prefixed with sf_ to avoid collisions) ---

    def sf_validate(self) -> "StructFrame":
        """Validates column existence, runtime dtypes, and field-level constraints."""
        actual_cols = set(self._sf_df.columns)
        missing = []

        for attr_name, meta in self.__class__._sf_schema.items():
            df_col = meta["df_col"]
            fi = meta["field_info"]
            inner_type = meta["inner_type"]

            # 1. Check existence
            if df_col not in actual_cols:
                if not meta["is_optional"]:
                    missing.append(df_col)
                continue

            series = self._sf_df[df_col]

            # 2. Check Runtime Dtypes
            if inner_type is not None:
                if inner_type == int and not ptypes.is_integer_dtype(series):
                    raise TypeError(
                        f"[{self.__class__.__name__}] Column '{df_col}' must be integer dtype."
                    )
                elif inner_type == float and not ptypes.is_float_dtype(series):
                    raise TypeError(
                        f"[{self.__class__.__name__}] Column '{df_col}' must be float dtype."
                    )
                elif inner_type == str and not (
                    ptypes.is_string_dtype(series) or ptypes.is_object_dtype(series)
                ):
                    raise TypeError(
                        f"[{self.__class__.__name__}] Column '{df_col}' must be string/object dtype."
                    )
                elif inner_type == bool and not ptypes.is_bool_dtype(series):
                    raise TypeError(
                        f"[{self.__class__.__name__}] Column '{df_col}' must be boolean dtype."
                    )

            # 3. Run Field-Level Validations
            if fi.ge is not None and not (series >= fi.ge).all():
                raise ValueError(
                    f"[Validation Error] Column '{df_col}' must be >= {fi.ge}"
                )

            if fi.le is not None and not (series <= fi.le).all():
                raise ValueError(
                    f"[Validation Error] Column '{df_col}' must be <= {fi.le}"
                )

            if fi.isin is not None and not series.isin(fi.isin).all():
                raise ValueError(
                    f"[Validation Error] Column '{df_col}' contains values not in {fi.isin}"
                )

        if missing:
            raise ValueError(
                f"[{self.__class__.__name__}] Missing required columns: {sorted(missing)}"
            )

        return self

    @property
    def sf_data(self) -> pd.DataFrame:
        """Escape hatch to retrieve the raw Pandas DataFrame."""
        return self._sf_df

    def sf_filter(self: TStructFrame, condition: pd.Series) -> TStructFrame:
        """Filters rows and safely returns a new instance of the structured object."""
        return self.__class__(self._sf_df[condition], copy=False, validate=False)

    # --- Python Protocols ---
    def __len__(self) -> int:
        return len(self._sf_df)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {len(self)} rows x {len(self._sf_df.columns)} cols>\n{self._sf_df.head()}"
