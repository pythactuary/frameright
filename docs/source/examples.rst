Examples
========

Insurance Risk Profile
----------------------

**With Pandas:**

.. code-block:: python

    from frameright.pandas import Schema, Col, Field
    from typing import Optional
    import pandas as pd

    class RiskProfile(Schema):  # Uses pandas backend
        """Schema for insurance risk data."""
        limit: Col[float] = Field(ge=0)
        """Policy limit."""
        attachment: Col[float] = Field(ge=0)
        """Attachment point."""
        premium: Col[float] = Field(gt=0)
        """Annual premium."""
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


**With Polars (eager, pl.Series):**

.. code-block:: python

    import polars as pl
    from frameright.polars.eager import Schema, Col, Field

    class RiskProfile(Schema):  # Uses polars eager backend
        limit: Col[float] = Field(ge=0)
        attachment: Col[float] = Field(ge=0)
        premium: Col[float] = Field(gt=0)
        currency: Col[str]

    df = pl.DataFrame({
        "limit": [1_000_000.0, 500_000.0],
        "attachment": [500_000.0, 250_000.0],
        "premium": [10_000.0, 5_000.0],
        "currency": ["USD", "EUR"],
    })

    risk = RiskProfile(df)  # Uses Polars eager backend
    excess = risk.attachment + risk.limit
    print(f"Max exposure: {excess.sum():,.0f}")

**With Polars Lazy (pl.Expr):**

.. code-block:: python

    import polars as pl
    from frameright.polars.lazy import Schema, Col, Field

    class LazyRiskProfile(Schema):  # Uses polars lazy backend
        limit: Col[float] = Field(ge=0)
        attachment: Col[float] = Field(ge=0)
        premium: Col[float] = Field(gt=0)
        currency: Col[str]

    df = pl.DataFrame({
        "limit": [1_000_000.0, 500_000.0],
        "attachment": [500_000.0, 250_000.0],
        "premium": [10_000.0, 5_000.0],
        "currency": ["USD", "EUR"],
    })

    # Example: build a lazy query
    lazy_df = df.lazy()
    lazy_risk = LazyRiskProfile(lazy_df)
    # All columns are pl.Expr, so you can use lazy polars methods
    filtered = lazy_risk.limit.filter(lazy_risk.limit > 600_000)
    # ...

Polars offers significant performance improvements for large datasets, especially with lazy evaluation.
Use ``frameright.polars.eager`` for eager (Series) or ``frameright.polars.lazy`` for lazy (Expr) schemas.
The same schema logic works for both backends.


Data Pipeline with Strict Contracts
------------------------------------

.. code-block:: python

    class RawOrders(Schema):
        order_id: Col[int] = Field(unique=True)
        item_price: Col[float] = Field(ge=0)
        quantity: Col[int] = Field(ge=1)

    class ProcessedOrders(Schema):
        order_id: Col[int] = Field(unique=True)
        item_price: Col[float] = Field(ge=0)
        quantity: Col[int] = Field(ge=1)
        revenue: Col[float] = Field(ge=0)

    def process_orders(raw: RawOrders) -> ProcessedOrders:
        \"\"\"Transform raw orders into processed orders with revenue.\"\"\"
        df = raw.fr_data.copy()
        df["revenue"] = df["item_price"] * df["quantity"]
        return ProcessedOrders(df)

    df = pd.read_csv("orders.csv")
    raw = RawOrders(df)
    processed = process_orders(raw)
    processed.fr_data.to_csv("processed_orders.csv", index=False)


Production Pattern: Validate at Boundaries
-----------------------------------------

When a DataFrame starts getting passed across modules and teams, a useful rule of thumb is:

* **Validate at I/O boundaries** (file reads, API inputs, database extracts)
* **Optionally skip validation inside a pipeline** (for performance), then
* **Re-validate at handoff boundaries** (what you return to other code)

.. code-block:: python

    from __future__ import annotations

    from frameright import Schema, Field
    from frameright.typing import Col

    class Claims(Schema):
        claim_id: Col[int] = Field(unique=True, nullable=False)
        """Stable claim identifier."""

        incurred: Col[float] = Field(gt=0)
        """Incurred loss (must be strictly positive for ratios)."""

        paid: Col[float] = Field(ge=0)
        """Paid loss."""

    class ClaimsWithLossRatio(Schema):
        claim_id: Col[int] = Field(unique=True, nullable=False)
        incurred: Col[float] = Field(gt=0)
        paid: Col[float] = Field(ge=0)
        loss_ratio: Col[float] = Field(ge=0)
        """Paid / incurred."""

    def add_loss_ratio(claims: Claims) -> ClaimsWithLossRatio:
        # Re-wrap without validating yet, because loss_ratio doesn't exist.
        out = ClaimsWithLossRatio(claims.fr_data, validate=False)
        out.loss_ratio = out.paid / out.incurred
        return out.fr_validate()

    # Boundary validation: fail fast on bad inputs
    df = pd.read_csv("claims.csv")
    claims = Claims(df)

    enriched = add_loss_ratio(claims)
    high_lr_df = enriched.fr_data[enriched.loss_ratio > 0.8]
    high_lr = ClaimsWithLossRatio(high_lr_df, validate=False)


Column Aliasing
---------------

.. code-block:: python

    class LegacyData(Schema):
        \"\"\"Map clean Python names to messy column names.\"\"\"
        user_id: Col[int] = Field(alias="USER_ID_V2")
        signup_date: Col[str] = Field(alias="dt_signup_YYYYMMDD")

    df = pd.DataFrame({
        "USER_ID_V2": [1, 2, 3],
        "dt_signup_YYYYMMDD": ["20240101", "20240215", "20240301"],
    })

    data = LegacyData(df)
    print(data.user_id)  # Accesses "USER_ID_V2" column
