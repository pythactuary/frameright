"""
ProteusFrame Usage Examples

This file demonstrates the key features of ProteusFrame:
- Multi-backend support (Pandas and Polars)
- Pandera-powered validation with constraints
- Type-safe column access with IDE autocomplete
- Backend-agnostic schema definitions
"""

from typing import Optional

import pandas as pd

from proteusframe import Field, ProteusFrame
from proteusframe.typing import Col


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


# Create sample data with Pandas
pandas_df = pd.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 101],
        "item_price": [15.50, 42.00, 9.99],
        "quantity_sold": [2, 1, 5],
    }
)

# Wrap with ProteusFrame (validates with Pandera on construction)
orders = OrderData(pandas_df)
print(f"✓ Created OrderData with {len(orders)} rows")
print(f"✓ Backend: {type(orders.pf_data).__name__}")

# Type-safe column access with full IDE autocomplete
orders.revenue = orders.item_price * orders.quantity_sold
total_revenue = orders.revenue.sum()
grouped = orders.pf_data.groupby(orders.customer_id)[[orders.revenue.name]].sum()
y = orders.pf_data
print(f"✓ Total Revenue: ${total_revenue:,.2f}")
