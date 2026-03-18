"""Polars eager (DataFrame) backend for FrameRight.

Import Schema and Col from this module when working with polars DataFrames:

    from frameright.polars.eager import Schema, Col, Field
    import polars as pl

    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    df = pl.DataFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]})
    orders = Orders(df)
    orders.revenue  # Returns pl.Series
"""

from typing import TYPE_CHECKING

import polars as pl

from frameright.backends.polars_eager_backend import PolarsEagerBackend
from frameright.core import BaseSchema, Field
from frameright.typing.polars_eager import Col


class Schema(BaseSchema):
    """Schema for polars eager DataFrames.

    Use this when working with polars DataFrames:

        import polars as pl
        from frameright.polars.eager import Schema, Col

        class Sales(Schema):
            customer: Col[str]
            revenue: Col[float]

        df = pl.DataFrame({"customer": ["Alice"], "revenue": [100.0]})
        sales = Sales(df)
        sales.revenue  # Returns pl.Series
    """

    _fr_backend = PolarsEagerBackend()

    def __init__(
        self,
        df: "pl.DataFrame",
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        coerce: bool = False,
        coerce_errors: str = "raise",
        strict: bool = False,
    ):
        """Initialise the polars eager Schema wrapper.

        Args:
            df: The polars DataFrame to wrap.
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
        def fr_data(self) -> "pl.DataFrame":
            """Return the underlying polars DataFrame."""
            ...


__all__ = ["Schema", "Col", "Field"]
