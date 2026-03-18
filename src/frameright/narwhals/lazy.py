"""Narwhals lazy (LazyFrame) backend for FrameRight.

Import Schema and Col from this module when working with narwhals LazyFrames:

    from frameright.narwhals.lazy import Schema, Col, Field
    import narwhals as nw
    import polars as pl

    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    lf = nw.from_native(pl.LazyFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]}))
    orders = Orders(lf)
    orders.revenue  # Returns nw.Expr (lazy expression)
"""

from typing import TYPE_CHECKING

import narwhals as nw

from frameright.backends.narwhals_backend import NarwhalsBackend
from frameright.core import BaseSchema, Field
from frameright.typing.narwhals import Col


class Schema(BaseSchema):
    """Schema for narwhals LazyFrames.

    Use this when working with narwhals lazy DataFrames:

        import narwhals as nw
        from frameright.narwhals.lazy import Schema, Col

        class Sales(Schema):
            customer: Col[str]
            revenue: Col[float]

        lf = nw.from_native(pl.LazyFrame(...))
        sales = Sales(lf)
        sales.revenue  # Returns nw.Expr (lazy expression)
    """

    _fr_backend = NarwhalsBackend()

    def __init__(
        self,
        df: "nw.LazyFrame",
        copy: bool = False,
        validate: bool = True,
        validate_types: bool = True,
        coerce: bool = False,
        coerce_errors: str = "raise",
        strict: bool = False,
    ):
        """Initialise the narwhals lazy Schema wrapper.

        Args:
            df: The narwhals LazyFrame to wrap.
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
        def fr_data(self) -> "nw.LazyFrame":
            """Return the underlying narwhals LazyFrame."""
            ...


__all__ = ["Schema", "Col", "Field"]
