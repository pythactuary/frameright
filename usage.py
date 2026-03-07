"""
ProteusFrame Usage Examples

This file demonstrates the key features of ProteusFrame:
- Multi-backend support (Pandas and Polars)
- Pandera-powered validation with constraints
- Type-safe column access with IDE autocomplete
- Backend-agnostic schema definitions
"""

import pandas as pd
from proteusframe import ProteusFrame, Field
from proteusframe.typing import Col
from typing import Optional

print("=" * 80)
print("ProteusFrame Usage Examples")
print("=" * 80)

# =============================================================================
# Example 1: Basic Schema with Pandas Backend
# =============================================================================
print("\n📊 Example 1: Basic Schema with Pandas")
print("-" * 80)


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
print(f"✓ Created OrderData with {len(orders.pf_data)} rows")
print(f"✓ Backend: {type(orders.pf_data).__name__}")

# Type-safe column access with full IDE autocomplete
orders.revenue = orders.item_price * orders.quantity_sold
total_revenue = orders.revenue.sum()
print(f"✓ Total Revenue: ${total_revenue:,.2f}")

# =============================================================================
# Example 2: Same Schema with Polars Backend
# =============================================================================
print("\n🚀 Example 2: Same Schema with Polars (High Performance)")
print("-" * 80)

try:
    import polars as pl

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
    orders_polars = OrderData(polars_df)
    print(f"✓ Created OrderData with {len(orders_polars.pf_data)} rows")
    print(f"✓ Backend: {type(orders_polars.pf_data).__name__}")

    # Same operations work seamlessly
    orders_polars.revenue = orders_polars.item_price * orders_polars.quantity_sold
    total_revenue_polars = orders_polars.revenue.sum()
    print(f"✓ Total Revenue: ${total_revenue_polars:,.2f}")
    print("✓ Backend detection is automatic - no configuration needed!")

except ImportError:
    print("⚠️  Polars not installed. Install with: pip install proteusframe[polars]")

# =============================================================================
# Example 3: Advanced Validation with Pandera Constraints
# =============================================================================
print("\n🔒 Example 3: Advanced Validation (Powered by Pandera)")
print("-" * 80)


class RiskProfile(ProteusFrame):
    """Insurance risk profile with strict validation constraints."""

    policy_id: Col[str] = Field(unique=True, min_length=5, max_length=20)
    """Unique policy identifier (5-20 characters)"""
    limit: Col[float] = Field(ge=0, le=10_000_000)
    """Policy limit ($0 - $10M)"""
    attachment: Col[float] = Field(ge=0)
    """Attachment point (must be non-negative)"""
    premium: Col[float] = Field(gt=0)
    """Annual premium (must be positive)"""
    currency: Col[str] = Field(isin=["USD", "EUR", "GBP"])
    """Currency code (restricted to USD, EUR, GBP)"""
    country: Optional[Col[str]]
    """Country code (optional)"""


# Valid data
risk_df = pd.DataFrame(
    {
        "policy_id": ["POL-12345", "POL-67890"],
        "limit": [1_000_000.0, 500_000.0],
        "attachment": [500_000.0, 250_000.0],
        "premium": [10_000.0, 5_000.0],
        "currency": ["USD", "EUR"],
    }
)

risk = RiskProfile(risk_df)
print(f"✓ Created RiskProfile with {len(risk.pf_data)} policies")
print(f"✓ All Pandera constraints passed: unique IDs, value ranges, currency enum")

# Calculate total exposure (attachment + limit)
total_exposure = risk.attachment + risk.limit
print(f"✓ Total exposure per policy: {total_exposure.tolist()}")

# =============================================================================
# Example 4: Column Aliasing for Legacy Data
# =============================================================================
print("\n🏷️  Example 4: Column Aliasing (Clean Names for Messy Columns)")
print("-" * 80)


class LegacyData(ProteusFrame):
    """Map clean Python names to messy legacy column names."""

    user_id: Col[int] = Field(alias="USER_ID_V2_FINAL")
    """User identifier"""
    signup_date: Col[str] = Field(alias="dt_signup_YYYYMMDD")
    """Date user signed up"""
    total_spent: Col[float] = Field(alias="TOTAL_SPEND_USD_2024", ge=0)
    """Total amount spent"""


legacy_df = pd.DataFrame(
    {
        "USER_ID_V2_FINAL": [1001, 1002, 1003],
        "dt_signup_YYYYMMDD": ["20240101", "20240215", "20240301"],
        "TOTAL_SPEND_USD_2024": [199.99, 549.99, 89.99],
    }
)

legacy = LegacyData(legacy_df)
print(f"✓ Created LegacyData with {len(legacy.pf_data)} users")

# Access via clean Python names (aliases map to ugly column names)
print(f"✓ User IDs: {legacy.user_id.tolist()}")
print(f"✓ Average spend: ${legacy.total_spent.mean():.2f}")
print("✓ Clean Python names, messy DataFrame columns - best of both worlds!")

# =============================================================================
# Example 5: Schema Introspection
# =============================================================================
print("\n🔍 Example 5: Schema Introspection")
print("-" * 80)

schema_info = OrderData.pf_schema_info()
print("OrderData Schema:")
for row in schema_info:
    print(
        f"  {row['attribute']:15s} {row['column']:15s} {row['type']:6s} required={row['required']}"
    )

# =============================================================================
# Example 5.5: Type-Safe Column Names with Series.name
# =============================================================================
print("\n✨ Example 5.5: Type-Safe Column Names")
print("-" * 80)

# When using underlying DataFrame operations (like groupby), avoid string literals!
# Use Series.name for type-safe column access with full IDE autocomplete


class SalesData(ProteusFrame):
    """Simple schema for demonstrating Series.name"""

    customer: Col[str]
    revenue: Col[float]


sales_df = pd.DataFrame(
    {
        "customer": ["Alice", "Bob", "Alice", "Charlie", "Bob"],
        "revenue": [100.0, 200.0, 150.0, 300.0, 250.0],
    }
)
sales = SalesData(sales_df)

# ❌ BAD: String literals (no autocomplete, typos not caught)
# customer_revenue = sales.pf_data.groupby('customer')['revenue'].sum()

# ✅ GOOD: Use Series.name (full IDE autocomplete, typos caught by type checker)
customer_revenue = sales.pf_data.groupby(sales.customer.name)[sales.revenue.name].sum()
print(f"✓ Revenue by customer (type-safe groupby): {dict(customer_revenue)}")


# Works with aliases too!
class AliasedData(ProteusFrame):
    """Schema with aliases to demonstrate Series.name with aliased columns."""

    user_id: Col[str] = Field(alias="USER_ID_V2")
    """The id of the user (aliased from USER_ID_V2)"""
    spend: Col[float] = Field(alias="TOT_SPEND_USD")


aliased_df = pd.DataFrame(
    {"USER_ID_V2": ["U1", "U2", "U1"], "TOT_SPEND_USD": [100.0, 200.0, 150.0]}
)
aliased = AliasedData(aliased_df)


# user_id.name returns "USER_ID_V2" (the actual DataFrame column name)
by_user = aliased.pf_data.groupby(aliased.user_id.name)[aliased.spend.name].sum()
print(f"✓ Spend by user (respects aliases): {dict(by_user)}")
print("✓ No string literals needed - IDE autocomplete works everywhere!")

# =============================================================================
# Example 6: Type-Safe Function Contracts
# =============================================================================
print("\n📝 Example 6: Type-Safe Function Contracts")
print("-" * 80)


class ProcessedOrders(ProteusFrame):
    """Output schema with computed fields."""

    order_id: Col[int] = Field(unique=True)
    revenue: Col[float] = Field(ge=0)
    profit_margin: Col[float]


def calculate_metrics(orders: OrderData) -> ProcessedOrders:
    """
    Transform raw orders into processed metrics.

    Type-safe: IDE knows what columns are available and will error
    on typos or wrong schema types at edit-time!
    """
    # Compute revenue using typed attribute access (full autocomplete)
    orders.revenue = orders.item_price * orders.quantity_sold

    # Select columns using Series.name — no string literals!
    result_df = orders.pf_data[[orders.order_id.name, orders.revenue.name]].copy()
    result_df = result_df.assign(profit_margin=0.25)

    return ProcessedOrders(result_df)


# Function signature tells you exactly what goes in and comes out
processed = calculate_metrics(orders)
print(f"✓ Processed {len(processed.pf_data)} orders with type-safe function")
print("✓ IDE autocomplete works on function parameters and return values")
print("✓ Static type checkers catch schema mismatches before runtime")

# =============================================================================
# Summary
# =============================================================================
print("\n" + "=" * 80)
print("🎉 Key Takeaways:")
print("=" * 80)
print("✓ Multi-backend: Same schema works with Pandas and Polars")
print("✓ Powered by Pandera: Production-grade validation with constraints")
print("✓ Type-safe: Full IDE autocomplete and static error checking")
print("✓ Backend-agnostic: Switch between Pandas/Polars without code changes")
print("✓ Clean API: Pydantic-like Field() constraints for readable schemas")
print("✓ No string literals: Use Series.name for type-safe column access everywhere")
print("=" * 80)
