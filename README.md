# StructFrame

[![Tests](https://github.com/yourusername/structframe/workflows/Tests/badge.svg)](https://github.com/yourusername/structframe/actions)
[![PyPI version](https://badge.fury.io/py/structframe.svg)](https://badge.fury.io/py/structframe)
[![Python Versions](https://img.shields.io/pypi/pyversions/structframe.svg)](https://pypi.org/project/structframe/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A lightweight Object-DataFrame Mapper (ODM) for Pandas.** Define your DataFrame schema as a Python class. Get full IDE autocomplete, catch column typos and type errors at edit-time with Pylance, Pyright, or mypy, validate data at runtime, and write self-documenting data contracts — all while keeping native Pandas speed.

---

## The Problem

Pandas DataFrames use string-based column lookups. This is fine for exploratory analysis, but in production code it means:

- **No autocomplete** — you have to remember every column name
- **Invisible to static type checkers** — Pylance, Pyright, and mypy cannot see inside `df["col"]` lookups. Typos like `df["reveneu"]` pass every check and only crash at runtime
- **No type information** — the IDE can't tell you what type a column contains
- **No documentation** — what does `"ltv_v2"` represent? Only the original author knows
- **No safe refactoring** — renaming a column means find-and-replace across the entire codebase

This creates enormous friction when DataFrames are passed between functions. Consider:

```python
def calculate_revenue(df: pd.DataFrame) -> pd.DataFrame:
    # What columns does df have? What types? What constraints?
    # We have no idea without reading the caller.
    df['total_revenue'] = df['item_price'] * df['quantity_sold']  # Typo? Who knows.
    return df
```

---

## The Solution

StructFrame lets you define your DataFrame schema as a standard Python class. Columns become real attributes with type hints and docstrings. Your IDE and static type checkers (Pylance, Pyright, mypy) treat them like any other Python class — meaning you get full autocomplete, hover documentation, and **compile-time error checking for column names and types**.

```python
from structframe import StructFrame, Field
from structframe.typing import Col
from typing import Optional

class Orders(StructFrame):
    item_price: Col[float]
    """The price per unit of the item."""
    quantity_sold: Col[int] = Field(ge=0)
    """Number of units sold."""
    revenue: Optional[Col[float]]
    """Computed revenue per line item."""

orders = Orders(df)

# Full IDE autocomplete — type "orders." and see all columns
# Hover to see type and docstring
orders.revenue = orders.item_price * orders.quantity_sold
total = orders.revenue.sum()
```

The real power is using these types deep in your codebase as strict contracts for function inputs and outputs:

```python
def calculate_revenue(orders: Orders) -> RevenueResult:
    # Self-documenting. The signature tells you exactly what goes in and comes out.
    ...
    return RevenueResult(...)
```

---

## What Static Type Checkers Catch — Without Running Your Code

Because StructFrame columns are real class attributes, **static type checkers catch errors at edit-time**, just as they would for any Python class. This works out of the box with:

- **Pylance / Pyright** (VS Code)
- **mypy** (CI / command line)
- **PyCharm's built-in checker**

No plugins, no extensions, no configuration. If your type checker can analyse a Python class, it can analyse a StructFrame:

```python
orders = Orders(df)

# Typo in column name
orders.reveneu
#       ^^^^^^^ Error: Cannot access attribute "reveneu" for class "Orders"

# Wrong schema type passed to function
def process(risk: RiskProfile) -> None: ...
process(orders)
#       ^^^^^^ Error: Argument of type "Orders" cannot be assigned to parameter "risk" of type "RiskProfile"

# Rename a column? Right-click → Rename Symbol. Done. Every reference updated.
```

Compare this to standard Pandas, where **none of these errors are caught**:

```python
df["reveneu"]       # No error. Silent KeyError at runtime.
process(wrong_df)   # No error. It's just a DataFrame.
# Rename a column? Good luck with find-and-replace across 50 files.
```

| Capability                           | Standard Pandas     | StructFrame                                    |
| ------------------------------------ | ------------------- | ---------------------------------------------- |
| Autocomplete on column names         | No                  | Yes                                            |
| Errors on typos (before running)     | No                  | Yes — via Pylance/mypy                         |
| Hover to see column type + docstring | No                  | Yes                                            |
| Rename-symbol refactoring            | No                  | Yes                                            |
| Find all references to a column      | No                  | Yes                                            |
| Type-safe function signatures        | No (`pd.DataFrame`) | Yes (`Orders` vs `Revenue` are distinct types) |

**This is StructFrame's core value.** It turns the entire Python static analysis ecosystem — Pylance, Pyright, mypy, PyCharm — into your DataFrame tooling layer. No plugins required. If your type checker can analyse a Python class, it can analyse a StructFrame.

---

## Installation

```bash
pip install structframe
```

---

## Quick Start

```python
import pandas as pd
from structframe import StructFrame, Field
from structframe.typing import Col
from typing import Optional

# 1. Define your schema
class OrderData(StructFrame):
    order_id: Col[int] = Field(unique=True)
    """Unique order identifier."""
    customer_id: Col[int]
    """Customer who placed the order."""
    item_price: Col[float] = Field(ge=0)
    """Price per unit (must be non-negative)."""
    quantity_sold: Col[int] = Field(ge=1)
    """Number of units sold (at least 1)."""
    revenue: Optional[Col[float]]
    """Computed revenue (optional — may not exist yet)."""

# 2. Wrap your data (validates on construction)
raw_df = pd.DataFrame({
    'order_id': [1, 2, 3],
    'customer_id': [101, 102, 101],
    'item_price': [15.50, 42.00, 9.99],
    'quantity_sold': [2, 1, 5]
})
orders = OrderData(raw_df)

# 3. Work with typed, documented columns
orders.revenue = orders.item_price * orders.quantity_sold
total = orders.revenue.sum()
```

---

## Key Features

### IDE-First Design

- **Autocomplete** — type `orders.` and see every column with its type and docstring
- **Static error checking** — Pylance/mypy catch typos and type mismatches before runtime
- **Hover documentation** — see column descriptions inline, no need to look up the schema
- **Rename symbol** — refactor column names across your entire codebase safely
- **Find all references** — see everywhere a column is used

### Runtime Validation

- **Type checking** — column dtypes are validated against annotations on construction
- **Field constraints** — Pydantic-style rules via `Field()`: `ge`, `gt`, `le`, `lt`, `isin`, `regex`, `min_length`, `max_length`, `nullable`, `unique`
- **Specific exceptions** — `MissingColumnError`, `TypeMismatchError`, `ConstraintViolationError`

### Data Handling

- **Column aliasing** — map clean attribute names to messy column names with `Field(alias="UGLY_COL_NAME")`
- **Optional columns** — `Optional[Col[T]]` for columns that may not exist; returns `None` safely
- **Type coercion** — `sf_coerce(messy_df)` auto-converts dtypes to match the schema
- **Schema introspection** — `sf_schema_info()` returns a DataFrame describing the schema
- **Factory methods** — `sf_from_csv()`, `sf_from_dict()`, `sf_from_records()`
- **Native Pandas speed** — column access maps directly to vectorized Pandas operations

---

## How It Compares

|                                 | **StructFrame**                     | **Pandera**                                                              | **Pydantic v2**                  |
| ------------------------------- | ----------------------------------- | ------------------------------------------------------------------------ | -------------------------------- |
| **Purpose**                     | Typed DataFrame access + validation | DataFrame validation                                                     | Row-oriented data validation     |
| **IDE autocomplete on columns** | Yes                                 | No — validator only, not an accessor                                     | N/A — no column concept          |
| **Static error checking**       | Yes — Pylance/mypy catch typos      | No — column names are still strings internally                           | N/A                              |
| **Column access**               | `orders.revenue` → `pd.Series`      | Not designed for column access                                           | Not designed for columnar data   |
| **Runtime validation**          | Yes                                 | Yes (more extensive: lazy validation, custom checks, hypothesis testing) | Yes (row-by-row)                 |
| **Performance at scale**        | Native Pandas (vectorized)          | Native Pandas (vectorized)                                               | Slow — O(n) model instantiations |
| **Backend**                     | Pandas                              | Pandas, Polars, PySpark, Modin, Dask                                     | Backend-agnostic (row-oriented)  |

**StructFrame is not a validation framework.** It is an access layer that makes DataFrames behave like typed Python objects. Pandera is the better choice for complex validation logic (custom checks, hypothesis testing, lazy error collection). They are complementary — you can use both.

---

## Advanced Usage

### Data Validation & Constraints

```python
class RiskProfile(StructFrame):
    limit: Col[float] = Field(ge=0, description="Policy limit")
    premium: Col[float] = Field(gt=0, description="Annual premium")
    currency: Col[str] = Field(isin=["USD", "EUR", "GBP"])
    country: Optional[Col[str]]

risk = RiskProfile(df)  # Validates immediately
```

### Column Aliasing

```python
class LegacyData(StructFrame):
    user_id: Col[int] = Field(alias="USER_ID_V2")
    signup_date: Col[str] = Field(alias="dt_signup_YYYYMMDD")

data = LegacyData(df)
print(data.user_id)  # Accesses "USER_ID_V2" column
```

### Type Coercion

```python
# CSV data where everything is a string
messy_df = pd.read_csv("data.csv")
orders = OrderData.sf_coerce(messy_df)  # Auto-converts dtypes
```

### Schema Introspection

```python
print(OrderData.sf_schema_info())
#   attribute      column   type  required  nullable  unique  constraints
# 0  order_id    order_id    int      True      True    True         None
# 1  customer_id customer_id int      True      True   False         None
# ...
```

### Safe Filtering

```python
# Returns a new OrderData instance, not a raw DataFrame
high_value = orders.sf_filter(orders.revenue > 50.00)
```

### The Escape Hatch

For complex Pandas operations like `.groupby()`, `.merge()`, or `.melt()`, use the underlying DataFrame directly:

```python
customer_totals = orders.sf_data.groupby('customer_id').sum()
```

### Generate Example Data

```python
# For testing and documentation
example = OrderData.sf_example(nrows=10)
```

---

## Exceptions

StructFrame raises specific exceptions for different failure modes:

| Exception                  | When It's Raised                                                  |
| -------------------------- | ----------------------------------------------------------------- |
| `MissingColumnError`       | A required column is not in the DataFrame                         |
| `TypeMismatchError`        | A column's dtype doesn't match the type annotation                |
| `ConstraintViolationError` | A `Field()` constraint is violated (e.g., `ge`, `isin`, `unique`) |

All inherit from `ValidationError` → `StructFrameError` → `Exception`.

---

## Contributing

Pull requests are welcome! If you find a bug or have a feature request, please open an issue.

```bash
# Development setup
git clone https://github.com/yourusername/structframe.git
cd structframe
pip install -e ".[dev]"
pytest
```

---

## License

MIT License — see [LICENSE](LICENSE) for details.
