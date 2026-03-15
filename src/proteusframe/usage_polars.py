# =============================================================================
# Example 2: Same Schema with Polars Backend
# =============================================================================

from typing import Optional

import polars as pl

from proteusframe import Field
from proteusframe import ProteusFramePolars as ProteusFrame
from proteusframe.typing.polars import Col


class OrderData(ProteusFrame):
    """Schema for e-commerce order data."""

    order_id: Col[int] = Field(unique=True)
    """Unique order identifier (must be unique)"""
    customer_id: Col[int]
    """Customer who placed the order"""
    item_price: Col[float] = Field(ge=0)
    """Price per unit (must be non-negative)"""
    quantity_sold: Col[int] = Field(ge=1)
    """Number of units sold (at least 1)"""
    revenue: Optional[Col[float]]
    """Computed revenue (optional column)"""


# Create the exact same data with Polars
polars_df = pl.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 101],
        "item_price": [15.50, 42.00, 9.99],
        "quantity_sold": [2, 1, 5],
    }
)

# Same schema class, different backend - completely automatic!
orders = OrderData(polars_df)
print(f"✓ Created OrderData with {len(orders)} rows")
print(f"✓ Backend: {type(orders.pf_data ).__name__}")

# Same operations work seamlessly
orders.revenue = orders.item_price * orders.quantity_sold
total_revenue_polars = orders.revenue.sum()
print(f"✓ Total Revenue: ${total_revenue_polars:,.2f}")
total_revenue_by_customver = orders.pf_data.group_by(orders.customer_id.name).sum()[
    orders.revenue.name
]
