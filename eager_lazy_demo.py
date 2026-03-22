"""Example demonstrating the new explicit eager/lazy import structure.

FrameRight now provides explicit imports for eager and lazy evaluation:

For Polars:
    from frameright.polars.eager import Schema, Col, Field  # For pl.DataFrame
    from frameright.polars.lazy import Schema, Col, Field   # For pl.LazyFrame

For Narwhals:
    from frameright.narwhals.eager import Schema, Col, Field  # For nw.DataFrame
    from frameright.narwhals.lazy import Schema, Col, Field   # For nw.LazyFrame

Backward compatibility is maintained:
    from frameright.polars import Schema, SchemaLazy, Col, Field
    from frameright.narwhals import Schema, SchemaLazy, Col, Field
"""

import narwhals as nw
import polars as pl

from frameright import Field

print("=" * 70)
print("POLARS EAGER (DataFrame)")
print("=" * 70)

from frameright.polars.eager import Col as EagerCol
from frameright.polars.eager import Schema as EagerSchema


class SalesEager(EagerSchema):
    customer: EagerCol[str]
    revenue: EagerCol[float] = Field(ge=0.0)
    quantity: EagerCol[int] = Field(ge=1)


df = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Charlie"],
        "revenue": [100.5, 250.0, 175.25],
        "quantity": [2, 5, 3],
    }
)

sales_eager = SalesEager(df)
print(f"✓ Created eager schema with {len(sales_eager)} rows")
print(f"  Type of sales_eager.revenue: {type(sales_eager.revenue)}")
print(f"  Revenue series: {sales_eager.revenue.to_list()}")
print(f"  Total revenue: {sales_eager.revenue.sum()}")

print("\n" + "=" * 70)
print("POLARS LAZY (LazyFrame)")
print("=" * 70)

from frameright.polars.lazy import Col as LazyCol
from frameright.polars.lazy import Schema as LazySchema


class SalesLazy(LazySchema):
    customer: LazyCol[str]
    revenue: LazyCol[float] = Field(ge=0.0)
    quantity: LazyCol[int] = Field(ge=1)


lf = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Charlie"],
        "revenue": [100.5, 250.0, 175.25],
        "quantity": [2, 5, 3],
    }
).lazy()

sales_lazy = SalesLazy(lf)
print(f"✓ Created lazy schema with {len(sales_lazy)} rows")
print(f"  Type of sales_lazy.revenue: {type(sales_lazy.revenue)}")
print(f"  Revenue expression: {sales_lazy.revenue}")
print(
    f"  Total revenue (collected): {sales_lazy.fr_data.select(sales_lazy.revenue.sum()).collect().item()}"
)

print("\n" + "=" * 70)
print("NARWHALS EAGER (DataFrame)")
print("=" * 70)

from frameright.narwhals.eager import Col as NwEagerCol
from frameright.narwhals.eager import Schema as NwEagerSchema


class SalesNwEager(NwEagerSchema):
    customer: NwEagerCol[str]
    revenue: NwEagerCol[float] = Field(ge=0.0)
    quantity: NwEagerCol[int] = Field(ge=1)


nw_df = nw.from_native(
    pl.DataFrame(
        {
            "customer": ["Alice", "Bob", "Charlie"],
            "revenue": [100.5, 250.0, 175.25],
            "quantity": [2, 5, 3],
        }
    )
)

sales_nw_eager = SalesNwEager(nw_df)
print(f"✓ Created narwhals eager schema with {len(sales_nw_eager)} rows")
print(f"  Type of sales_nw_eager.revenue: {type(sales_nw_eager.revenue)}")

print("\n" + "=" * 70)
print("BACKWARD COMPATIBILITY")
print("=" * 70)

from frameright.polars import Schema, SchemaLazy


class SalesCompat(Schema):  # Will use eager backend
    customer: EagerCol[str]
    revenue: EagerCol[float]


class SalesCompatLazy(SchemaLazy):  # Will use lazy backend
    customer: LazyCol[str]
    revenue: LazyCol[float]


print("✓ Backward compatible imports still work")
print(f"  Schema backend: {SalesCompat._fr_backend.__class__.__name__}")
print(f"  SchemaLazy backend: {SalesCompatLazy._fr_backend.__class__.__name__}")

print("\n" + "=" * 70)
print("KEY DIFFERENCES")
print("=" * 70)
print(
    """
Eager (DataFrame):
  - Columns return pl.Series or nw.Series (materialized data)
  - Immediate computation
  - Best for: Interactive analysis, small datasets

Lazy (LazyFrame):
  - Columns return pl.Expr or nw.Expr (query expressions)
  - Deferred computation with optimization
  - Best for: Large datasets, complex transformations

Import Pattern:
  OLD: from frameright.polars import Schema, SchemaLazy
  NEW: from frameright.polars.eager import Schema, Col
       from frameright.polars.lazy import Schema, Col
"""
)
