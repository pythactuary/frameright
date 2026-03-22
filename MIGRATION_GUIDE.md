# Migration Guide: Explicit Eager/Lazy Imports

## Overview

FrameRight now provides explicit import paths for eager and lazy evaluation modes. This gives you better type safety and clearer code.

## What Changed

### Before (Old API)

```python
from frameright.polars import Schema, SchemaLazy, Col

class Sales(Schema):  # For pl.DataFrame
    revenue: Col[float]

class SalesLazy(SchemaLazy):  # For pl.LazyFrame
    revenue: Col[float]
```

### After (New API)

```python
from frameright.polars.eager import Schema, Col
from frameright.polars.lazy import Schema as SchemaLazy, Col as ColLazy

class Sales(Schema):  # For pl.DataFrame
    revenue: Col[float]  # Col is pl.Series

class SalesLazy(SchemaLazy):  # For pl.LazyFrame
    revenue: ColLazy[float]  # ColLazy is pl.Expr
```

## Recommended Import Patterns

### Polars Eager (DataFrame)

```python
from frameright.polars.eager import Schema, Col, Field

class MyData(Schema):
    value: Col[float]

df = pl.DataFrame({"value": [1.0, 2.0, 3.0]})
data = MyData(df)
data.value  # Returns pl.Series
```

### Polars Lazy (LazyFrame)

```python
from frameright.polars.lazy import Schema, Col, Field

class MyData(Schema):
    value: Col[float]

lf = pl.DataFrame({"value": [1.0, 2.0, 3.0]}).lazy()
data = MyData(lf)
data.value  # Returns pl.Expr
```

### Narwhals Eager (DataFrame)

```python
from frameright.narwhals.eager import Schema, Col, Field

class MyData(Schema):
    value: Col[float]

nw_df = nw.from_native(df)
data = MyData(nw_df)
data.value  # Returns nw.Series
```

### Narwhals Lazy (LazyFrame)

```python
from frameright.narwhals.lazy import Schema, Col, Field

class MyData(Schema):
    value: Col[float]

nw_lf = nw.from_native(lf, eager_only=False)
data = MyData(nw_lf)
data.value  # Returns nw.Expr
```

### Pandas (Always Eager)

```python
from frameright.pandas import Schema, Col, Field

class MyData(Schema):
    value: Col[float]

df = pd.DataFrame({"value": [1.0, 2.0, 3.0]})
data = MyData(df)
data.value  # Returns pd.Series
```

## Backward Compatibility

The old import patterns still work for backward compatibility:

```python
# Old style - still works
from frameright.polars import Schema, SchemaLazy, Col, Field

class Sales(Schema):  # Uses eager backend
    revenue: Col[float]

class SalesLazy(SchemaLazy):  # Uses lazy backend
    revenue: Col[float]
```

However, we recommend migrating to the new explicit imports for better type safety.

## Benefits of New API

1. **Explicit is better than implicit**: Import path clearly shows eager vs lazy
2. **Better type safety**: `Col` types match the actual return type (Series vs Expr)
3. **IDE autocomplete**: Separate Col types give better IDE suggestions
4. **Clearer code**: No need for separate SchemaLazy class naming convention

## Module Structure

```
frameright/
├── pandas/
│   └── __init__.py      # Schema, Col, Field (always eager)
├── polars/
│   ├── __init__.py      # Re-exports for backward compatibility
│   ├── eager.py         # Schema, Col for pl.DataFrame → pl.Series
│   └── lazy.py          # Schema, Col for pl.LazyFrame → pl.Expr
└── narwhals/
    ├── __init__.py      # Re-exports for backward compatibility
    ├── eager.py         # Schema, Col for nw.DataFrame → nw.Series
    └── lazy.py          # Schema, Col for nw.LazyFrame → nw.Expr
```

## Backend Detection

FrameRight automatically detects the correct backend:

- `pl.DataFrame` → `polars` backend (eager)
- `pl.LazyFrame` → `polars_lazy` backend (lazy)
- `nw.DataFrame` → `narwhals` backend (eager)
- `nw.LazyFrame` → `narwhals` backend (lazy)
- `pd.DataFrame` → `pandas` backend (always eager)

## Examples

### Eager Polars with Type Safety

```python
from frameright.polars.eager import Schema, Col, Field
import polars as pl

class Orders(Schema):
    order_id: Col[int]
    revenue: Col[float] = Field(ge=0.0)

df = pl.DataFrame({
    "order_id": [1, 2, 3],
    "revenue": [100.0, 200.0, 150.0]
})

orders = Orders(df)

# Column access returns pl.Series
revenue_series: pl.Series = orders.revenue
print(revenue_series.sum())  # 450.0

# Use polars operations directly
high_revenue = df.filter(orders.revenue > 150.0)
```

### Lazy Polars with Expression Building

```python
from frameright.polars.lazy import Schema, Col, Field
import polars as pl

class Orders(Schema):
    order_id: Col[int]
    revenue: Col[float] = Field(ge=0.0)

lf = pl.DataFrame({
    "order_id": [1, 2, 3],
    "revenue": [100.0, 200.0, 150.0]
}).lazy()

orders = Orders(lf)

# Column access returns pl.Expr
revenue_expr: pl.Expr = orders.revenue

# Build lazy query
result = (
    lf
    .filter(orders.revenue > 150.0)
    .select(orders.order_id, orders.revenue)
    .collect()
)
```

## Testing Your Migration

1. Update imports to use explicit eager/lazy paths
2. Run your type checker (mypy, pyright, etc.)
3. Fix any type errors related to Col types
4. Run your test suite
5. Verify IDE autocomplete works correctly

## Questions?

If you encounter any issues during migration, please open an issue on GitHub with:

- Your current code
- The error or unexpected behavior
- Your FrameRight version
