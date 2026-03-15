"""
ProteusFrame Narwhals Backend Demo

Demonstrates ProteusFrame's backend support for Narwhals (backend-agnostic DataFrames):
- Uses ProteusFrameNarwhals for explicit narwhals backend
- Columns are nw.Series, supporting backend-agnostic methods
- Same code works with any backend (pandas, polars, duckdb, etc.)
"""

from typing import Optional

import narwhals as nw
import polars as pl

from proteusframe import Field
from proteusframe import ProteusFrameNarwhals as ProteusFrame
from proteusframe.typing.narwhals import Col as Col  # For narwhals autocomplete


class Data(ProteusFrame):
    """Backend-agnostic schema using narwhals."""

    customer: Col[str]
    revenue: Col[float] = Field(ge=0.0)
    profit: Optional[Col[float]]


# Wrap pandas DataFrame with narwhals (could be any backend)
nw_df = nw.from_native(
    pl.DataFrame(
        {
            "customer": ["Alice", "Bob", "Charlie"],
            "revenue": [100.0, 200.0, 150.0],
        }
    )
)

sales = Data(nw_df)
print(f"✓ Backend: {sales.pf_backend.name}")
print(f"✓ Column type: {type(sales.revenue)}")
print(f"✓ Is nw.Series?: {isinstance(sales.revenue, nw.Series)}")

# Use backend-agnostic narwhals methods
print(f"✓ Total revenue (narwhals .sum()): {sales.revenue.sum()}")
print(f"✓ Mean revenue (narwhals .mean()): {sales.revenue.mean():.2f}")

# Modify column with narwhals operations (works with any backend)
sales.profit = sales.revenue * 0.3  # nw.Series arithmetic
print(f"✓ Computed profit (nw.Series): {sales.profit.to_list()}")
print(f"✓ Computed profit (nw.Series): {sales.profit.to_list()}")
print(f"✓ Computed profit (nw.Series): {sales.profit.to_list()}")
