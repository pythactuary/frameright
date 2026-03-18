"""Polars eager (DataFrame) backend for FrameRight.

Import Schema, Col, and Field from this module when working with polars DataFrames:

    from frameright.polars_eager import Schema, Col, Field
    import polars as pl

    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    df = pl.DataFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]})
    orders = Orders(df)
    orders.revenue  # Returns pl.Series
    orders.revenue.sum()  # Use polars Series methods
"""

from typing import TYPE_CHECKING

import polars as pl

from frameright.backends.polars_eager_backend import PolarsEagerBackend
from frameright.core import BaseSchema, Field
from frameright.typing.polars_eager import Col


class Schema(BaseSchema):
    """Schema for polars eager DataFrames.

    Use this when working with polars DataFrames (eager evaluation):

        import polars as pl
        from frameright.polars_eager import Schema, Col

        class Sales(Schema):
            customer: Col[str]
            revenue: Col[float]

        df = pl.DataFrame({"customer": ["Alice"], "revenue": [100.0]})
        sales = Sales(df)
        sales.revenue  # Returns pl.Series
        sales.revenue.sum()  # Returns concrete value
    """

    _fr_backend = PolarsEagerBackend()

    if TYPE_CHECKING:

        @property
        def fr_data(self) -> "pl.DataFrame":
            """Return the underlying polars DataFrame."""
            ...


__all__ = ["Schema", "Col", "Field"]
