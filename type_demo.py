"""
Demo showing improved type hints for IDE support.

Run this file in VS Code with Pylance enabled to see autocomplete
working perfectly for fr_data operations.

Note: This uses the pandas backend.
For other backends, use:
  - from frameright.polars.eager import Schema
  - from frameright.polars.lazy import Schema
  - from frameright.narwhals.eager import Schema
"""

import pandas as pd

from frameright import Field
from frameright.pandas import Col, Schema


class Sales(Schema):  # Uses pandas backend (imported from frameright.pandas)
    """Sales data with descriptions."""

    order_id: Col[int] = Field(unique=True)
    """Unique order identifier"""
    customer: Col[str]
    """Customer name"""
    revenue: Col[float] = Field(ge=0)
    """Revenue in USD"""


# Create sample data
df = pd.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer": ["Alice", "Bob", "Alice"],
        "revenue": [100.0, 200.0, 150.0],
    }
)

sales = Sales(df)

print("=" * 80)
print("Schema Type Safety Demo")
print("=" * 80)

# ✅ fr_data returns pd.DataFrame with full autocomplete
# Because we parameterized Schema[pd.DataFrame], Pylance knows
# sales.fr_data is a pd.DataFrame — no type narrowing needed!
print("\n1️⃣  Simple operations on fr_data:")
print(f"   Shape: {sales.fr_data.shape}")
print(f"   Columns: {list(sales.fr_data.columns)}")

# ✅ Chained operations work with full autocomplete
print("\n2️⃣  Chained operations — full autocomplete, no tricks needed:")
customer_totals = sales.fr_data.groupby("customer").aggregate("revenue").sum()
print(f"   Customer totals: {customer_totals.to_dict()}")
print(f"   Return type: {type(customer_totals).__name__}")

# ✅ Combine with Series.name for zero string literals
print("\n3️⃣  Type-safe column names (no string literals!):")
by_customer = sales.fr_data.groupby(str(sales.customer.name))[
    str(sales.revenue.name)
].sum()
print(f"   By customer: {by_customer.to_dict()}")

# ✅ Schema introspection returns plain dicts
print("\n4️⃣  Schema introspection (list of dicts):")
schema = Sales.fr_schema_info()
for row in schema:
    print(
        f"   {row['attribute']:12s} type={row['type']:5s} {row.get('description', '')}"
    )

# ✅ Instance preserves docstring
print(f"\n5️⃣  Instance docstring preserved: '{sales.__doc__}'")

# ✅ Unparameterized Schema still works (fr_data returns Any)
print("\n6️⃣  Unparameterized usage (defaults to pd.DataFrame):")


class BasicSales(Schema):
    """No type parameter — fr_data defaults to pd.DataFrame."""

    customer: Col[str]
    revenue: Col[float]


basic = BasicSales(df[["customer", "revenue"]])
print(f"   fr_data type at runtime: {type(basic.fr_data).__name__}")
print("   Full pd.DataFrame autocomplete — no type parameter needed!")

print("\n" + "=" * 80)
print("✨ Key Points:")
print("=" * 80)
print("✅ Schema: fr_data defaults to pd.DataFrame autocomplete")
print("✅ Schema[pl.DataFrame]: fr_data has full polars autocomplete")
print("✅ One change in class definition — all code stays backend-agnostic")
print("=" * 80)
