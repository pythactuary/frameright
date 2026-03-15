"""
ProteusFrame Polars Eager Demo

Demonstrates ProteusFrame's backend support for Polars Eager (pl.Series):
- Uses Col for static typing and IDE autocomplete
- All columns are pl.Series, supporting eager polars methods
"""

from typing import Optional

import polars as pl

from proteusframe import Field
from proteusframe import ProteusFramePolars as ProteusFrame
from proteusframe.typing.polars_eager import Col


class SalesEager(ProteusFrame):
    """Sales data schema for Polars Eager (pl.Series)."""

    customer: Col[str]
    revenue: Col[float] = Field(ge=0.0)
    profit: Optional[Col[float]]


# Create with polars DataFrame
df = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Charlie"],
        "revenue": [100.0, 200.0, 150.0],
    }
)

sales = SalesEager(df)
print(f"✓ Backend: {sales.pf_backend.name}")
print(f"✓ Column type: {type(sales.revenue)}")
print(f"✓ Is pl.Series?: {isinstance(sales.revenue, pl.Series)}")

# Use native polars methods
print(f"✓ Total revenue (polars .sum()): {sales.revenue.sum()}")
print(f"✓ Mean revenue (polars .mean()): {sales.revenue.mean():.2f}")

# Modify column with polars operations

# Assign profit as a Col[float] (pl.Series) for type checkers
sales.profit = sales.revenue * 0.3
if sales.profit is not None:
    print(f"✓ Computed profit (pl.Series): {sales.profit.to_list()}")
    print(f"✓ Computed profit (pl.Series): {sales.profit.to_list()}")
    print(f"✓ Computed profit (pl.Series): {sales.profit.to_list()}")
