"""
ProteusFrame Polars Lazy Demo

Demonstrates ProteusFrame's backend support for Polars Lazy (pl.Expr):
- Uses Col from polars_lazy for static typing and IDE autocomplete
- All columns are pl.Expr, supporting lazy polars methods
"""

from typing import Optional

import polars as pl

from proteusframe import Field
from proteusframe import ProteusFramePolarsLazy as ProteusFrame
from proteusframe.typing.polars_lazy import Col


class Sales(ProteusFrame):
    """Sales data schema for Polars Lazy (pl.Expr)."""

    customer: Col[str]
    revenue: Col[float] = Field(ge=0.0)
    profit: Optional[Col[float]]


# Create with polars lazy DataFrame
lazy_df = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Charlie"],
        "revenue": [100.0, 200.0, 150.0],
    }
).lazy()

sales = Sales(lazy_df)
print(f"✓ Backend: {sales.pf_backend.name}")
print(f"✓ Column type: {type(sales.revenue)}")
print(f"✓ Is pl.Expr?: {hasattr(sales.revenue, 'alias')}")

# Use native polars lazy methods (e.g., .sum() returns an Expr, not a value)
total_revenue_expr = sales.revenue.sum()
print(f"✓ Total revenue expr (lazy .sum()): {total_revenue_expr}")

# Modify column with polars lazy operations
sales.profit = sales.revenue * 0.3  # pl.Expr arithmetic
print(f"✓ Computed profit expr (pl.Expr): {sales.profit}")
print(f"✓ Computed profit expr (pl.Expr): {sales.profit}")
print(f"✓ Computed profit expr (pl.Expr): {sales.profit}")
