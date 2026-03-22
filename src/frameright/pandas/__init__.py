"""Pandas backend for FrameRight.

Import Schema, Col, and Field from this module when working with pandas:

    from frameright.pandas import Schema, Col, Field
    import pandas as pd

    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    df = pd.DataFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]})
    orders = Orders(df)
    orders.revenue  # Returns pd.Series
"""

from typing import TYPE_CHECKING

import pandas as pd

from frameright.backends.pandas_backend import PandasBackend
from frameright.core import BaseSchema, Field
from frameright.typing import Col as _RuntimeCol

# Re-export for convenience
Col = _RuntimeCol  # type: ignore[misc]

# ------------------------------------------------------------------
# Concrete Backend-Specific Schema
# ------------------------------------------------------------------


class Schema(BaseSchema):
    """Schema for pandas DataFrames.

    Use this when working with pandas:

        import pandas as pd
        from frameright.pandas import Schema, Col

        class Sales(Schema):
            customer: Col[str]
            revenue: Col[float]

        df = pd.DataFrame({"customer": ["Alice"], "revenue": [100.0]})
        sales = Sales(df)
        sales.revenue  # Returns pd.Series
    """

    _fr_backend = PandasBackend()

    def __init__(
        self,
        df: "pd.DataFrame",
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        coerce: bool = False,
        coerce_errors: str = "raise",
        strict: bool = False,
    ):
        """Initialise the pandas Schema wrapper.

        Args:
            df: The pandas DataFrame to wrap.
            copy: If True, copy the DataFrame. Defaults to False to save memory.
            validate: If True, run schema validation on construction. Defaults to True.
            validate_types: If True, also check runtime dtypes during validation.
                            Only used when ``validate`` is True.
            coerce: If True, attempt to convert DataFrame columns to match the schema's
                    type annotations before validation. Defaults to False.
            coerce_errors: How to handle coercion errors when ``coerce`` is True.
                          'raise' (default), 'coerce' (set failures to NaN), or 'ignore'.
            strict: If True, reject DataFrames with columns not defined in the schema.
                    Defaults to False (extra columns are allowed).
        """
        super().__init__(
            df,
            copy=copy,
            validate=validate,
            validate_types=validate_types,
            coerce=coerce,
            coerce_errors=coerce_errors,
            strict=strict,
        )

    if TYPE_CHECKING:

        @property
        def fr_data(self) -> "pd.DataFrame":
            """Return the underlying pandas DataFrame."""
            ...


__all__ = ["Schema", "Field", "Col"]
