"""
Demo showing improved type hints for IDE support.

Run this file in VS Code with Pylance enabled to see autocomplete
working perfectly for pf_data operations.
"""

from proteusframe import ProteusFrame, Field
from proteusframe.typing import Col
import pandas as pd


class Sales(ProteusFrame[pd.DataFrame]):
    """Sales data with descriptions."""

    order_id: Col[int] = Field(unique=True)
    """Unique order identifier"""
    customer: Col[str]
    """Customer name"""
    revenue: Col[float] = Field(ge=0)
    """Revenue in USD"""


# Create sample data
df = pd.DataFrame(
    {"order_id": [1, 2, 3], "customer": ["Alice", "Bob", "Alice"], "revenue": [100.0, 200.0, 150.0]}
)

sales = Sales(df)

print("=" * 80)
print("ProteusFrame Type Safety Demo")
print("=" * 80)

# ✅ pf_data returns pd.DataFrame with full autocomplete
# Because we parameterized ProteusFrame[pd.DataFrame], Pylance knows
# sales.pf_data is a pd.DataFrame — no type narrowing needed!
print("\n1️⃣  Simple operations on pf_data:")
print(f"   Shape: {sales.pf_data.shape}")
print(f"   Columns: {list(sales.pf_data.columns)}")

# ✅ Chained operations work with full autocomplete
print("\n2️⃣  Chained operations — full autocomplete, no tricks needed:")
customer_totals = sales.pf_data.groupby("customer")["revenue"].sum()
print(f"   Customer totals: {customer_totals.to_dict()}")
print(f"   Return type: {type(customer_totals).__name__}")

# ✅ Combine with Series.name for zero string literals
print("\n3️⃣  Type-safe column names (no string literals!):")
by_customer = sales.pf_data.groupby(sales.customer.name)[sales.revenue.name].sum()
print(f"   By customer: {by_customer.to_dict()}")

# ✅ Schema introspection returns plain dicts
print("\n4️⃣  Schema introspection (list of dicts):")
schema = Sales.pf_schema_info()
for row in schema:
    print(f"   {row['attribute']:12s} type={row['type']:5s} {row.get('description', '')}")

# ✅ Instance preserves docstring
print(f"\n5️⃣  Instance docstring preserved: '{sales.__doc__}'")

# ✅ Unparameterized ProteusFrame still works (pf_data returns Any)
print("\n6️⃣  Unparameterized usage (defaults to pd.DataFrame):")


class BasicSales(ProteusFrame):
    """No type parameter — pf_data defaults to pd.DataFrame."""

    customer: Col[str]
    revenue: Col[float]


basic = BasicSales(df[["customer", "revenue"]])
print(f"   pf_data type at runtime: {type(basic.pf_data).__name__}")
print(f"   Full pd.DataFrame autocomplete — no type parameter needed!")

print("\n" + "=" * 80)
print("✨ Key Points:")
print("=" * 80)
print("✅ ProteusFrame: pf_data defaults to pd.DataFrame autocomplete")
print("✅ ProteusFrame[pl.DataFrame]: pf_data has full polars autocomplete")
print("✅ One change in class definition — all code stays backend-agnostic")
print("=" * 80)
