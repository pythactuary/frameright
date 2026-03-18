"""Polars backend for FrameRight.

For explicit eager/lazy separation, use:
    from frameright.polars.eager import Schema, Col, Field
    from frameright.polars.lazy import Schema, Col, Field

For backward compatibility:
    from frameright.polars import Schema, SchemaLazy, Col, Field

    # For eager DataFrames
    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    df = pl.DataFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]})
    orders = Orders(df)
    orders.revenue  # Returns pl.Series

    # For LazyFrames
    class OrdersLazy(SchemaLazy):
        order_id: Col[int]
        revenue: Col[float]

    lf = pl.DataFrame({"order_id": [1, 2], "revenue": [100.0, 200.0]}).lazy()
    orders_lazy = OrdersLazy(lf)
    orders_lazy.revenue  # Returns pl.Expr
"""

from frameright.core import Field

# Re-export Col from eager (most common use case)
from frameright.polars.eager import Col, Schema
from frameright.polars.lazy import Schema as SchemaLazy

__all__ = ["Schema", "SchemaLazy", "Col", "Field"]
