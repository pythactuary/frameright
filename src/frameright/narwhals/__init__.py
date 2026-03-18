"""Narwhals backend for FrameRight.

For explicit eager/lazy separation, use:
    from frameright.narwhals.eager import Schema, Col, Field
    from frameright.narwhals.lazy import Schema, Col, Field

For backward compatibility:
    from frameright.narwhals import Schema, SchemaLazy, Col, Field

    # For eager DataFrames
    class Orders(Schema):
        order_id: Col[int]
        revenue: Col[float]

    df = nw.from_native(pd.DataFrame(...))
    orders = Orders(df)
    orders.revenue  # Returns nw.Series

    # For LazyFrames
    class OrdersLazy(SchemaLazy):
        order_id: Col[int]
        revenue: Col[float]

    lf = nw.from_native(pl.LazyFrame(...))
    orders_lazy = OrdersLazy(lf)
    orders_lazy.revenue  # Returns nw.Expr
"""

from frameright.core import Field

# Re-export Col from eager (most common use case)
from frameright.narwhals.eager import Col, Schema
from frameright.narwhals.lazy import Schema as SchemaLazy

__all__ = ["Schema", "SchemaLazy", "Col", "Field"]
