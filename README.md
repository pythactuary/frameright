# ProteusFrame

<p align="center">
  <img src="logo-banner.svg" alt="ProteusFrame - Type-Safe, Multi-Backend DataFrames" width="100%">
</p>

<p align="center">
  <a href="https://github.com/yourusername/proteusframe/actions"><img src="https://github.com/yourusername/proteusframe/workflows/Tests/badge.svg" alt="Tests"></a>
  <a href="./coverage-badge.svg"><img src="./coverage-badge.svg" alt="Coverage"></a>
  <a href="https://badge.fury.io/py/proteusframe"><img src="https://badge.fury.io/py/proteusframe.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/proteusframe/"><img src="https://img.shields.io/pypi/pyversions/proteusframe.svg" alt="Python Versions"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
</p>

**A lightweight Object-DataFrame Mapper (ODM) for Pandas, Polars, and Narwhals.** Define your DataFrame schema as a Python class with typed attributes. Get full IDE autocomplete, catch column typos and type errors at edit-time with Pylance, Pyright, or mypy, validate data at runtime with production-grade Pandera validation, and write self-documenting data contracts — all while keeping native backend speed and APIs.

**Native Backend Support**: Works with **Pandas**, **Polars**, and **Narwhals** DataFrames. Column properties return native Series types (`pd.Series`, `pl.Series`, or `nw.Series`), so you get the full native API for your backend. ProteusFrame is an Object DataFrame Mapper — it provides typed attribute access to columns, not DataFrame abstraction.

**Multi-Backend or Backend-Agnostic**: Choose your style:

- Use **Pandas** → get `pd.Series` with pandas methods
- Use **Polars** → get `pl.Series` with polars methods
- Use **Narwhals** → get `nw.Series` for backend-agnostic code

**Powered by Pandera**: Runtime validation uses [Pandera](https://pandera.readthedocs.io/) under the hood, giving you production-tested constraint checking with helpful error messages. ProteusFrame wraps Pandera's validation in a cleaner API while adding IDE-first typed column access.

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

ProteusFrame lets you define your DataFrame schema as a standard Python class. Columns become real attributes with type hints and docstrings. Your IDE and static type checkers (Pylance, Pyright, mypy) treat them like any other Python class — meaning you get full autocomplete, hover documentation, and **compile-time error checking for column names and types**.

```python
from proteusframe import ProteusFrame, Field
from proteusframe.typing import Col
from typing import Optional

class Orders(ProteusFrame):
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

Because ProteusFrame columns are real class attributes, **static type checkers catch errors at edit-time**, just as they would for any Python class. This works out of the box with:

- **Pylance / Pyright** (VS Code)
- **mypy** (CI / command line)
- **PyCharm's built-in checker**

No plugins, no extensions, no configuration. If your type checker can analyse a Python class, it can analyse a ProteusFrame:

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

| Capability                           | Standard Pandas     | ProteusFrame                                   |
| ------------------------------------ | ------------------- | ---------------------------------------------- |
| Autocomplete on column names         | No                  | Yes                                            |
| Errors on typos (before running)     | No                  | Yes — via Pylance/mypy                         |
| Hover to see column type + docstring | No                  | Yes                                            |
| Rename-symbol refactoring            | No                  | Yes                                            |
| Find all references to a column      | No                  | Yes                                            |
| Type-safe function signatures        | No (`pd.DataFrame`) | Yes (`Orders` vs `Revenue` are distinct types) |

**This is ProteusFrame's core value.** It turns the entire Python static analysis ecosystem — Pylance, Pyright, mypy, PyCharm — into your DataFrame tooling layer. No plugins required. If your type checker can analyse a Python class, it can analyse a ProteusFrame.

---

## Installation

```bash
# For Pandas backend (default, always installed)
pip install proteusframe

# For Polars backend (optional)
pip install proteusframe[polars]

# For Narwhals backend-agnostic support (optional)
pip install proteusframe[narwhals]
```

**Backend Selection** — Choose your approach:

**Option 1: Backend-Specific Classes (Recommended for Type Safety)**

```python
from proteusframe import ProteusFramePandas, ProteusFramePolars, ProteusFramePolarsLazy

class Sales(ProteusFramePandas):    # Always uses pandas
    revenue: Col[float]

class Sales(ProteusFramePolars):     # Always uses polars eager
    revenue: Col[float]

class Sales(ProteusFramePolarsLazy): # Always uses polars lazy
    revenue: Col[float]
```

**Option 2: Base ProteusFrame (Defaults to Pandas)**

```python
from proteusframe import ProteusFrame

class Sales(ProteusFrame):  # Defaults to pandas backend
    revenue: Col[float]

sales = Sales(df)                        # Uses pandas (default)
sales = Sales(polars_df, backend="polars")  # Explicitly use polars
```

**What you get**:

- `pd.DataFrame` → columns return `pd.Series`
- `pl.DataFrame` → columns return `pl.Series`
- `pl.LazyFrame` → columns return `pl.Expr` (lazy expressions)
- `nw.DataFrame` → columns return `nw.Series` (backend-agnostic)

---

## Quick Start

**With Pandas:**

```python
import pandas as pd
from proteusframe import ProteusFrame, Field
from proteusframe.typing import Col
from typing import Optional

# 1. Define your schema
class OrderData(ProteusFrame):
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

# 2. Wrap your data (validates with Pandera on construction)
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

**With Polars (recommended: use backend-specific class):**

```python
import polars as pl
from proteusframe import ProteusFramePolars, Field  # Backend-specific class
from proteusframe.typing.polars_eager import Col  # Polars eager (Series) type hints
from typing import Optional

# Same schema definition (for brevity, showing abbreviated version)
class OrderData(ProteusFramePolars):  # Explicitly uses Polars backend
    order_id: Col[int] = Field(unique=True)
    item_price: Col[float] = Field(ge=0)
    quantity_sold: Col[int] = Field(ge=1)
    revenue: Optional[Col[float]]

raw_df = pl.DataFrame({
    'order_id': [1, 2, 3],
    'customer_id': [101, 102, 101],
    'item_price': [15.50, 42.00, 9.99],
    'quantity_sold': [2, 1, 5]
})

# Uses Polars backend (set by ProteusFramePolars base class)
orders = OrderData(raw_df)
orders.revenue = orders.item_price * orders.quantity_sold  # pl.Series operations
total = orders.revenue.sum()  # Polars .sum() method
```

**Alternative: Use base ProteusFrame with backend parameter:**

```python
from proteusframe import ProteusFrame  # Base class

class OrderData(ProteusFrame):  # Defaults to pandas
    ...

# Explicitly specify polars backend
orders = OrderData(raw_df, backend="polars")
```

**With Narwhals (backend-agnostic code):**

```python
import narwhals as nw
import pandas as pd  # or polars, duckdb, etc.
from proteusframe import ProteusFrameNarwhals, Field  # Backend-specific class
from proteusframe.typing.narwhals import Col  # Narwhals-specific type hints
from typing import Optional

# Schema with narwhals types
class OrderData(ProteusFrameNarwhals):  # Explicitly uses Narwhals backend
    order_id: Col[int] = Field(unique=True)
    item_price: Col[float] = Field(ge=0)
    quantity_sold: Col[int] = Field(ge=1)
    revenue: Optional[Col[float]]

# Wrap any DataFrame with narwhals for backend-agnostic operations
raw_df = pd.DataFrame({'order_id': [1, 2, 3], ...})
nw_df = nw.from_native(raw_df)

# Uses Narwhals backend
orders = OrderData(nw_df)
orders.revenue = orders.item_price * orders.quantity_sold  # nw.Series operations
total = orders.revenue.sum()  # Backend-agnostic .sum()
```

**Type Hints Matter**: Use the appropriate `Col` import for your backend to get IDE autocomplete:

- `from proteusframe.typing import Col` → pandas methods (default)
- `from proteusframe.typing.polars_eager import Col` → polars eager (Series) methods
- `from proteusframe.typing.polars_lazy import Col` → polars lazy (Expr) methods
- `from proteusframe.typing.narwhals import Col` → narwhals methods

**Typing note (important):** Pandas has mature type stubs, so type checkers can treat attribute accessors like `orders.amount` as `pd.Series[float]` in many cases.
Polars and Narwhals do not currently expose fully generic `Series[T]` / `Expr[T]` types upstream, so type checkers typically see `pl.Series` / `pl.Expr` / `nw.Series` (inner type is best-effort today). ProteusFrame still gives you safe column _names_, schema-level `Col[T]` annotations, and IDE autocomplete.

**Type Safety:** Backend-specific classes like `ProteusFramePandas` give you the strongest type guarantees. The base `ProteusFrame` is more flexible but defaults to pandas for backward compatibility.

**Using Native DataFrame Operations:**

ProteusFrame is an **Object DataFrame Mapper** — it provides typed attribute access to columns, not DataFrame abstraction. Use native DataFrame methods for operations:

```python
# Filtering - use native methods
filtered_orders = OrderData(orders.pf_data[orders.revenue > 100], validate=False)  # Pandas
filtered_orders = OrderData(orders.pf_data.filter(orders.revenue > 100), validate=False)  # Polars

# Exporting - use native methods
orders.pf_data.to_csv('output.csv')        # Pandas
orders.pf_data.write_csv('output.csv')     # Polars

# Grouping - use native methods
orders.pf_data.groupby(orders.customer_id).sum()  # Pandas
orders.pf_data.group_by(orders.customer_id).sum()  # Polars

# LazyFrame collection - use native methods
lazy_orders = OrderData(pl.scan_csv('orders.csv'))  # Still lazy
result = lazy_orders.pf_data.collect()               # Executes query plan
```

The `pf_data` property gives you direct access to the underlying DataFrame.

---

## Key Features

### IDE-First Design

- **Autocomplete** — type `orders.` and see every column with its type and docstring
- **Static error checking** — Pylance/mypy catch typos and type mismatches before runtime
- **Hover documentation** — see column descriptions inline, no need to look up the schema
- **Rename symbol** — refactor column names across your entire codebase safely
- **Find all references** — see everywhere a column is used

### Runtime Validation (Powered by Pandera)

- **Production-grade validation** — uses [Pandera](https://pandera.readthedocs.io/), the industry-standard DataFrame validation library
- **Type checking** — column dtypes are validated against annotations on construction
- **Field constraints** — Pydantic-style rules via `Field()`: `ge`, `gt`, `le`, `lt`, `isin`, `regex`, `min_length`, `max_length`, `nullable`, `unique`
- **Helpful error messages** — clear, actionable validation failures with row/column context
- **Specific exceptions** — `MissingColumnError`, `TypeMismatchError`, `ConstraintViolationError`
- **Multi-backend** — works with `pandera.pandas` for Pandas and `pandera.polars` for Polars

### Data Handling

- **Native backend support** — works with Pandas, Polars, and Narwhals DataFrames transparently
- **Native Series types** — column properties return native types: `pd.Series`, `pl.Series`, or `nw.Series`
- **Full backend API** — use native methods on columns (e.g., `orders.revenue.mean()` uses pandas/polars/narwhals methods)
- **Object DataFrame Mapper** — ProteusFrame maps typed attributes to columns, it doesn't abstract the DataFrame
- **Column aliasing** — map clean attribute names to messy column names with `Field(alias="UGLY_COL_NAME")`
- **Optional columns** — `Optional[Col[T]]` for columns that may not exist; returns `None` safely
- **Type coercion** — `pf_coerce(messy_df)` auto-converts dtypes to match the schema
- **Schema introspection** — `pf_schema_info()` returns a list of dicts describing the schema
- **Native backend speed** — column access maps directly to vectorized operations (pandas, polars, or narwhals)

---

## How It Compares

|                                 | **ProteusFrame**                                                  | **Pandera**                                                              | **Pydantic v2**                  |
| ------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------ | -------------------------------- |
| **Purpose**                     | Typed DataFrame access (ODM) + validation                         | DataFrame validation                                                     | Row-oriented data validation     |
| **IDE autocomplete on columns** | Yes                                                               | No — validator only, not an accessor                                     | N/A — no column concept          |
| **Static error checking**       | Yes — Pylance/mypy catch typos                                    | No — column names are still strings internally                           | N/A                              |
| **Column access**               | `orders.revenue` → native `pd.Series` / `pl.Series` / `nw.Series` | Not designed for column access                                           | Not designed for columnar data   |
| **Runtime validation**          | Yes (uses Pandera internally)                                     | Yes (more extensive: lazy validation, custom checks, hypothesis testing) | Yes (row-by-row)                 |
| **Performance at scale**        | Native backend (vectorized)                                       | Native backend (vectorized)                                              | Slow — O(n) model instantiations |
| **Backend**                     | Pandas, Polars, Narwhals                                          | Pandas, Polars, PySpark, Modin, Dask                                     | Backend-agnostic (row-oriented)  |
| **Developer experience**        | Pydantic-like API + strong typing                                 | Schema-centric, validation-focused                                       | Models over DataFrames           |

**ProteusFrame uses Pandera for validation.** Think of ProteusFrame as a developer-friendly wrapper around Pandera that adds IDE-first typed column access and a Pydantic-like API. You get the best of both worlds: production-tested validation from Pandera plus the ergonomics of typed Python classes.

For advanced Pandera features (custom checks, hypothesis testing, lazy error collection), use Pandera directly alongside ProteusFrame (for example by validating `obj.pf_data` or by applying additional Pandera checks in your pipeline).

---

## Why Polars?

Polars is a modern DataFrame library written in Rust that offers significant performance advantages over Pandas:

- **10-100x faster** for many operations, especially on larger datasets (1M+ rows)
- **Lazy evaluation** — build complex query plans that are optimized before execution
- **Parallel execution** — automatically uses all CPU cores
- **Memory efficiency** — better memory layout and columnar processing
- **Growing ecosystem** — rapidly becoming the standard for high-performance data processing in Python

ProteusFrame makes it trivial to switch between Pandas and Polars. Define your schema once, and you can use whichever backend fits your needs:

```python
# Development: use Pandas for its familiar API and extensive ecosystem
df_pandas = pd.read_csv("data.csv")
orders = OrderData(df_pandas)

# Production: use Polars for better performance on large datasets
df_polars = pl.read_csv("data.csv")
orders = OrderData(df_polars)  # Same schema, different backend
```

No code changes required. The schema class is backend-agnostic.

## Why Narwhals?

[Narwhals](https://narwhals-dev.github.io/narwhals/) is a lightweight compatibility layer that lets you write backend-agnostic DataFrame code. It provides a unified API that works across Pandas, Polars, DuckDB, and more.

**When to use Narwhals with ProteusFrame:**

- You're building a **library** that needs to work with any DataFrame type
- You want to write **portable data pipelines** that work unchanged on Pandas, Polars, or DuckDB
- You need to **switch backends** without rewriting code

**ProteusFrame's Approach to Narwhals:**

ProteusFrame is an **Object DataFrame Mapper**, not a DataFrame abstraction layer. This means:

- **Native backends preferred**: If you're using Pandas, use the PandasBackend and get `pd.Series` with pandas methods
- **Native backends preferred**: If you're using Polars, use the PolarsBackend and get `pl.Series` with polars methods
- **Narwhals for portability**: If you need backend-agnostic code, use the NarwhalsBackend and get `nw.Series`

```python
# Pandas users: get pd.Series with pandas API
import pandas as pd
from proteusframe.typing import Col  # defaults to pd.Series[T]

# Polars users: get pl.Series with polars eager API
import polars as pl
from proteusframe.typing.polars_eager import Col  # pl.Series (with polars autocomplete)

# Backend-agnostic users: get nw.Series with narwhals API
import narwhals as nw
from proteusframe.typing.narwhals import Col  # nw.Series (with narwhals autocomplete)
```

**The value of ProteusFrame is typed attribute access**, not DataFrame abstraction. Choose the backend that fits your needs, and ProteusFrame gives you typed column access with full IDE support.

## Why Pandera?

[Pandera](https://pandera.readthedocs.io/) is the industry-standard DataFrame validation library with:

- **Battle-tested** — used by thousands of data teams in production
- **Clear error messages** — get actionable feedback with row/column context when validation fails
- **Extensive validation** — supports complex constraints, custom checks, and hypothesis testing
- **Multi-backend** — works with Pandas, Polars, PySpark, Modin, and Dask
- **Active development** — regular releases and strong community support

ProteusFrame leverages Pandera for all runtime validation, giving you production-grade constraint checking without the boilerplate. You get:

```python
class Orders(ProteusFrame):
    order_id: Col[int] = Field(unique=True)       # Pandera checks uniqueness
    item_price: Col[float] = Field(ge=0)          # Pandera checks >= 0
    currency: Col[str] = Field(isin=["USD", "EUR"])  # Pandera checks enum

orders = Orders(df)  # Automatic Pandera validation
```

Behind the scenes, ProteusFrame builds a Pandera schema from your class definition and runs validation on construction. You get all of Pandera's power with a cleaner, more Pydantic-like API.

---

## Advanced Usage

### Data Validation & Constraints

```python
class RiskProfile(ProteusFrame):
    limit: Col[float] = Field(ge=0)
    """Policy limit."""
    premium: Col[float] = Field(gt=0)
    """Annual premium."""
    currency: Col[str] = Field(isin=["USD", "EUR", "GBP"])
    country: Optional[Col[str]]

risk = RiskProfile(df)  # Validates immediately
```

### Column Aliasing

```python
class LegacyData(ProteusFrame):
    user_id: Col[int] = Field(alias="USER_ID_V2")
    signup_date: Col[str] = Field(alias="dt_signup_YYYYMMDD")

data = LegacyData(df)
print(data.user_id)  # Accesses "USER_ID_V2" column
```

### Type Coercion

```python
# CSV data where everything is a string
messy_df = pd.read_csv("data.csv")
orders = OrderData.pf_coerce(messy_df)  # Auto-converts dtypes
```

### Schema Introspection

```python
for col in OrderData.pf_schema_info():
    print(col["attribute"], col["type"], col["required"])
# order_id     int   True
# customer_id  int   True
# ...
```

Returns a list of dicts with keys: `attribute`, `column`, `type`, `required`, `nullable`, `unique`, `constraints`, `description`.

### The Escape Hatch

For complex operations like `.groupby()`, `.merge()`, or `.melt()`, you need access to a DataFrame object.

ProteusFrame provides an **escape hatche**:

**`pf_data` — Get the underlying dataframe**

```python
import narwhals as nw

orders = OrderData(df)

# pf_data returns a narwhals DataFrame — same API for any backend
# Full IDE autocomplete via type stubs, zero-copy wrapper
nw_df = orders.pf_data
print(nw_df.columns)                    # ['order_id', 'customer_id', 'revenue']
print(nw_df.schema)                     # Column names → narwhals dtypes
filtered = nw_df.filter(nw.col('revenue') > 100)
grouped = nw_df.group_by('customer_id').agg(nw.col('revenue').sum())
```

### Type-Safe Group Keys

When working with native DataFrames, you can often pass the **column object itself** (recommended), instead of spelling a column name:

```python
# No string literals, no `.name` needed
customer_totals = orders.pf_data.groupby(orders.customer_id).sum()  # Pandas
customer_totals = orders.pf_data.group_by(orders.customer_id).sum()  # Polars
```

This also respects aliases automatically because the grouping key is derived from the real column:

```python
class LegacyData(ProteusFrame):
    user_id: Col[int] = Field(alias="USER_ID_V2")
    total_spent: Col[float] = Field(alias="TOT_SPEND_USD")

data = LegacyData(df)

by_user = data.pf_data.groupby(data.user_id).sum()  # Pandas
```

**Benefits:**

- IDE autocomplete on `orders.customer_id`
- Static type checkers catch typos
- Respects aliases automatically
- No string literals anywhere in your code

### Generate Example Data

```python
# For testing and documentation
example = OrderData.pf_example(nrows=10)
```

---

## Exceptions

ProteusFrame raises specific exceptions for different failure modes:

| Exception                  | When It's Raised                                                  |
| -------------------------- | ----------------------------------------------------------------- |
| `MissingColumnError`       | A required column is not in the DataFrame                         |
| `TypeMismatchError`        | A column's dtype doesn't match the type annotation                |
| `ConstraintViolationError` | A `Field()` constraint is violated (e.g., `ge`, `isin`, `unique`) |

All inherit from `ValidationError` → `ProteusFrameError` → `Exception`.

---

## Contributing

Pull requests are welcome! If you find a bug or have a feature request, please open an issue.

```bash
# Development setup
git clone https://github.com/yourusername/proteusframe.git
cd proteusframe
pip install -e ".[dev,polars]"

# Run tests
make test

# Run tests with coverage report
make coverage

# Generate coverage badge (update before committing)
make badge

# Run type checking
make lint
```

The coverage badge is automatically generated from test results. After making changes that affect coverage, run `make badge` to update [coverage-badge.svg](coverage-badge.svg) before committing.

---

## License

MIT License — see [LICENSE](LICENSE) for details.
