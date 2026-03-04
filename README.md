# StructFrame

**A lightweight Object-DataFrame Mapper (ODM) for Pandas.** Get strict data contracts, perfect IDE autocomplete, and zero string-lookup boilerplate, all while keeping 100% of Pandas' native speed.

---

## The Problem
When working with wide datasets in Pandas, relying on "magic strings" for column names is a massive pain point. It breaks IDE autocomplete, invites silent typo bugs, and makes large data pipelines hard to refactor.

**The Old Way (Standard Pandas):**
```python
# No autocomplete, prone to typos, visually cluttered
df['total_revenue'] = df['item_price'] * df+'quantity_sold']
```

## The Solution: StructFrame
StructFrame allows you to define your DataFrame schema as a standard Python class using type hints. It automatically binds those attributes to the underlying Pandas data matrix.

**The StructFrame Way:**
```python
# Perfect IDE autocomplete, completely type-safe, beautiful syntax
orders.total_revenue = orders.item_price * orders.quantity_sold
```
---

## Installation

bash
pip install structframe


---

## Quick Start

Define your strict data contract by inheriting from `StructFrame` and using standard Python type hints for your columns.

```python
import pandas as pd
from structframe import StructFrame

# 1. Define your strict data schema
class OrderData(StructFrame):
    order_id: pd.Series
    customer_id: pd.Series
    item_price: pd.Series
    quantity_sold: pd.Series
    total_revenue: pd.Series

# 2. Load your raw Pandas data
raw_df = pd.DataFrame({
    'order_id': ['A1', 'A2', 'A3'],
    'customer_id': [101, 102, 101],
    'item_price': [15.50, 42.00, 9.99],
    'quantity_sold': [2, 1, 5]
})

# 3. Instantiate your structured object
orders = OrderData(raw_df)

# 4. Write pure, algebraic Python (Your IDE will autocomplete everything!)
orders.total_revenue = orders.item_price * orders.quantity_sold

print(orders.total_revenue)
```

---

## Key Features

* **Perfect IDE Autocomplete:** Because `StructFrame` relies on static type hints, your IDE (VS Code, PyCharm, etc.) knows exactly what columns exist. Type `orders.` and watch your columns appear in the dropdown.
* **Strict Data Contracts:** Catch missing columns immediately. Pass `strict=True` during initialization, and `StructFrame` will raise a clear `ValueError` if the incoming DataFrame doesn't match your schema.
* **Native Pandas Speed:** Under the hood, operations map directly to vectorized Pandas calls. There is zero performance penalty.
* **Zero Boilerplate:** No need to write dozens of property getters and setters. `StructFrame` uses a metaclass hook to wire up everything dynamically.

---

## Advanced Usage

### Data Validation
Ensure bad data never enters your pipeline:
```python
# Raises a ValueError if raw_df is missing 'item_price' or 'quantity_sold'
orders = OrderData(raw_df, strict=True)
```

### Safe Filtering
When you need to slice data, use the `.filter()`Method to ensure the resulting subset remains a strictly typed `StructFrame` object, rather than falling back to a raw DataFrame.
```python
high_value_orders = orders.filter(orders.total_revenue > 50.00)
```

### Serialization
Easily save and load your structured data:
```python
# Save to disk
orders.to_parquet("sales_data.parquet")

# Load directly back into your structured schema
new_orders = OrderData.from_parquet("sales_data.parquet")
```

### The Escape Hatch
If you need to perform complex Pandas operations like `.groupby()`, `.merge()`, or `.melt()`, you can always grab the perfectly aligned underlying DataFrame via the `.data` property:
```python
customer_totals = orders.data.groupby('customer_id').sum()
```

---

## Contributing
Pull requests are welcome! If you find a bug or have a feature request, please open an issue.