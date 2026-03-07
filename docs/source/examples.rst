Examples
========

Insurance Risk Profile
----------------------

**With Pandas:**

.. code-block:: python

    from proteusframe import ProteusFrame, Field
    from proteusframe.typing import Col
    from typing import Optional
    import pandas as pd

    class RiskProfile(ProteusFrame):
        """Schema for insurance risk data."""
        limit: Col[float] = Field(ge=0, description="Policy limit")
        attachment: Col[float] = Field(ge=0, description="Attachment point")
        premium: Col[float] = Field(gt=0, description="Annual premium")
        currency: Col[str] = Field(isin=["USD", "EUR", "GBP"])
        country: Optional[Col[str]]

    df = pd.DataFrame({
        "limit": [1_000_000.0, 500_000.0],
        "attachment": [500_000.0, 250_000.0],
        "premium": [10_000.0, 5_000.0],
        "currency": ["USD", "EUR"],
    })

    risk = RiskProfile(df)
    excess = risk.attachment + risk.limit
    print(f"Max exposure: {excess.sum():,.0f}")

**With Polars (same schema, better performance):**

.. code-block:: python

    import polars as pl

    # Same schema definition as above
    df = pl.DataFrame({
        "limit": [1_000_000.0, 500_000.0],
        "attachment": [500_000.0, 250_000.0],
        "premium": [10_000.0, 5_000.0],
        "currency": ["USD", "EUR"],
    })

    risk = RiskProfile(df)  # Automatically uses Polars backend
    excess = risk.attachment + risk.limit
    print(f"Max exposure: {excess.sum():,.0f}")

Polars offers significant performance improvements for large datasets, especially with lazy evaluation.
The same schema works for both backends — just pass in the DataFrame type you prefer.


Data Pipeline with Strict Contracts
------------------------------------

.. code-block:: python

    class RawOrders(ProteusFrame):
        order_id: Col[int] = Field(unique=True)
        item_price: Col[float] = Field(ge=0)
        quantity: Col[int] = Field(ge=1)

    class ProcessedOrders(ProteusFrame):
        order_id: Col[int] = Field(unique=True)
        item_price: Col[float] = Field(ge=0)
        quantity: Col[int] = Field(ge=1)
        revenue: Col[float] = Field(ge=0)

    def process_orders(raw: RawOrders) -> ProcessedOrders:
        \"\"\"Transform raw orders into processed orders with revenue.\"\"\"
        df = raw.pf_data.copy()
        df["revenue"] = df["item_price"] * df["quantity"]
        return ProcessedOrders(df)

    raw = RawOrders.pf_from_csv("orders.csv")
    processed = process_orders(raw)
    processed.pf_to_csv("processed_orders.csv")


Column Aliasing
---------------

.. code-block:: python

    class LegacyData(ProteusFrame):
        \"\"\"Map clean Python names to messy column names.\"\"\"
        user_id: Col[int] = Field(alias="USER_ID_V2")
        signup_date: Col[str] = Field(alias="dt_signup_YYYYMMDD")

    df = pd.DataFrame({
        "USER_ID_V2": [1, 2, 3],
        "dt_signup_YYYYMMDD": ["20240101", "20240215", "20240301"],
    })

    data = LegacyData(df)
    print(data.user_id)  # Accesses "USER_ID_V2" column
