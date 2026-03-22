"""
FrameRight - Simple Examples

This file demonstrates the new import pattern where you explicitly
import the backend you're using.
"""

# Example 1: Pandas backend
print("=" * 60)
print("Example 1: Pandas Backend")
print("=" * 60)

import pandas as pd

from frameright.pandas import Col, Field, Schema


class Orders(Schema):
    """Order data schema for pandas."""

    order_id: Col[int] = Field(unique=True)
    customer_id: Col[int]
    revenue: Col[float] = Field(ge=0)


# Create pandas DataFrame
df_pandas = pd.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 101],
        "revenue": [150.0, 200.0, 99.99],
    }
)

# Wrap with Schema
orders_pd = Orders(df_pandas)
print(f"✓ Created {type(orders_pd).__name__} with {len(orders_pd)} rows")
print(f"✓ Backend: {type(orders_pd.fr_data).__name__}")
print(f"✓ Total revenue: ${orders_pd.revenue.sum():,.2f}")
print(f"✓ Column access returns: {type(orders_pd.revenue).__name__}")
print()

# Example 2: Polars eager backend
print("=" * 60)
print("Example 2: Polars Eager Backend")
print("=" * 60)

import polars as pl

from frameright.polars.eager import Col as PolarsCol
from frameright.polars.eager import Schema as PolarsSchema


class OrdersPolars(PolarsSchema):
    """Order data schema for polars."""

    order_id: PolarsCol[int]
    customer_id: PolarsCol[int]
    revenue: PolarsCol[float]


# Create polars DataFrame
df_polars = pl.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 101],
        "revenue": [150.0, 200.0, 99.99],
    }
)

# Wrap with Schema
orders_pl = OrdersPolars(df_polars)
print(f"✓ Created {type(orders_pl).__name__} with {len(orders_pl)} rows")
print(f"✓ Backend: {type(orders_pl.fr_data).__name__}")
print(f"✓ Total revenue: ${orders_pl.revenue.sum():,.2f}")
print(f"✓ Column access returns: {type(orders_pl.revenue).__name__}")
print()

# Example 3: Native DataFrame operations
print("=" * 60)
print("Example 3: Native DataFrame Operations")
print("=" * 60)

# Pandas groupby
pd_grouped = orders_pd.fr_data.groupby("customer_id")["revenue"].sum()
print(f"✓ Pandas groupby result:\n{pd_grouped}")
print()

# Polars groupby
pl_grouped = orders_pl.fr_data.group_by("customer_id").agg(pl.col("revenue").sum())
print(f"✓ Polars groupby result:\n{pl_grouped}")
print()

print("=" * 60)
print("Key Principles")
print("=" * 60)
print("1. FrameRight is an Object DataFrame Mapper (ODM)")
print("2. It provides typed schema access, NOT DataFrame abstraction")
print("3. Use native pandas/polars operations via .fr_data")
print("4. Import from backend-specific modules for best type safety")
print("=" * 60)
print("1. FrameRight is an Object DataFrame Mapper (ODM)")
print("2. It provides typed schema access, NOT DataFrame abstraction")
print("3. Use native pandas/polars operations via .fr_data")
print("4. Import from backend-specific modules for best type safety")
