"""Polars lazy (LazyFrame) backend for FrameRight.

Import Schema and Col from this module when working with polars LazyFrames:

    from frameright.polars.lazy import Schema, Col, Field
    import polars as pl

    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    lf = pl.DataFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]}).lazy()
    orders = Orders(lf)
    orders.revenue  # Returns pl.Expr (lazy expression)
"""

from typing import TYPE_CHECKING

import polars as pl

from frameright.backends.polars_lazy_backend import PolarsLazyBackend
from frameright.core import BaseSchema, Field
from frameright.typing.polars_lazy import Col


class Schema(BaseSchema):
    """Schema for polars LazyFrames.

    Use this when working with polars LazyFrames:

        import polars as pl
        from frameright.polars.lazy import Schema, Col

        class Sales(Schema):
            customer: Col[str]
            revenue: Col[float]

        lf = pl.DataFrame({"customer": ["Alice"], "revenue": [100.0]}).lazy()
        sales = Sales(lf)
        sales.revenue  # Returns pl.Expr (lazy expression)
    """

    _fr_backend = PolarsLazyBackend()

    def __init__(
        self,
        df: "pl.LazyFrame",
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        coerce: bool = False,
        coerce_errors: str = "raise",
        strict: bool = False,
    ):
        """Initialise the polars lazy Schema wrapper.

        Args:
            df: The polars LazyFrame to wrap.
            copy: If True, copy the LazyFrame. Defaults to False to save memory.
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
        def fr_data(self) -> "pl.LazyFrame":
            """Return the underlying polars LazyFrame."""
            ...


__all__ = ["Schema", "Col", "Field"]
