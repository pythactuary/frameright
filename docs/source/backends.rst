Backend Support
===============

ProteusFrame supports multiple DataFrame backends with automatic detection and backend-agnostic schemas.

Supported Backends
------------------

* **Pandas** — Mature ecosystem, extensive third-party library support
* **Polars** — High-performance, Rust-based, with lazy evaluation

The same schema definition works with both backends. You don't need to change your code.

Automatic Backend Detection
----------------------------

ProteusFrame automatically detects which backend you're using based on the DataFrame type:

.. code-block:: python

    import pandas as pd
    import polars as pl
    from proteusframe import ProteusFrame, Field
    from proteusframe.typing import Col

    class Orders(ProteusFrame):
        order_id: Col[int] = Field(unique=True)
        revenue: Col[float] = Field(ge=0)

    # Pandas backend
    pandas_df = pd.DataFrame({...})
    orders_pd = Orders(pandas_df)  # Uses PandasBackend

    # Polars backend
    polars_df = pl.DataFrame({...})
    orders_pl = Orders(polars_df)  # Uses PolarsBackend

No configuration needed. Backend selection is transparent.

Typing Notes
------------

ProteusFrame schemas are backend-agnostic, but you can opt into backend-specific typing for a better IDE experience:

* Pandas: ``from proteusframe.typing.pandas import Col, Index`` (columns type-check as ``pd.Series[T]``)
* Polars: ``from proteusframe.typing.polars import Col, Index`` (columns type-check as ``pl.Expr`` for expression chaining)

At runtime, the actual values you get depend on the backend:

* Pandas: properties return ``pd.Series``
* Polars eager ``pl.DataFrame``: properties return ``pl.Series``
* Polars lazy ``pl.LazyFrame``: properties return ``pl.Expr`` (lazy expressions)

Pandas Backend
--------------

**Installation:**

.. code-block:: bash

    pip install proteusframe

Pandas comes as a default dependency.

**Features:**

* Full validation with ``pandera.pandas``
* Access to the entire Pandas ecosystem
* Familiar API for existing Pandas users
* Great for exploratory analysis and data science workflows

**Example:**

.. code-block:: python

    import pandas as pd

    df = pd.read_csv("data.csv")
    orders = Orders(df)

    # Use any Pandas method via pf_data
    customer_totals = orders.pf_data.groupby("customer_id").sum()

Polars Backend
--------------

**Installation:**

.. code-block:: bash

    pip install proteusframe[polars]

**Why Polars?**

* **10-100x faster** than Pandas on large datasets (1M+ rows)
* **Parallel execution** — uses all CPU cores automatically
* **Lazy evaluation** — build optimized query plans
* **Memory efficient** — better memory layout and columnar processing
* **Modern API** — expressive, consistent, and type-safe

**Example:**

.. code-block:: python

    import polars as pl

    df = pl.read_csv("data.csv")
    orders = Orders(df)

    # Access Polars operations via pf_data
    customer_totals = orders.pf_data.group_by("customer_id").sum()

**Lazy Evaluation:**

Polars supports lazy evaluation for complex query optimization:

.. code-block:: python

    # LazyFrame is automatically handled
    lazy_df = pl.scan_csv("data.csv")
    orders = Orders(lazy_df)  # ProteusFrame works with LazyFrames too

    # Operations are lazy until you collect()
    filtered = orders.pf_filter(orders.revenue > 1000)
    # Execute the full query plan
    result = filtered.pf_data.collect()

Backend-Agnostic Schemas
-------------------------

The key benefit: **write your schema once, use it with any backend**.

.. code-block:: python

    class SalesData(ProteusFrame):
        """Works with both Pandas and Polars."""
        date: Col[str]
        product: Col[str]
        revenue: Col[float] = Field(ge=0)
        quantity: Col[int] = Field(ge=1)

    # Use with Pandas during development
    dev_df = pd.read_csv("sample.csv")
    dev_data = SalesData(dev_df)

    # Switch to Polars in production for better performance
    prod_df = pl.read_csv("full_dataset.csv")
    prod_data = SalesData(prod_df)

This means you can:

* Prototype with Pandas (familiar, extensive library ecosystem)
* Scale with Polars (performance, parallelism, memory efficiency)
* Never rewrite your schema definitions

Validation with Pandera
------------------------

Both backends use **Pandera** for validation:

* Pandas backend uses ``pandera.pandas``
* Polars backend uses ``pandera.polars``

The validation logic is identical. Pandera automatically handles backend-specific validation:

.. code-block:: python

    class Validated(ProteusFrame):
        amount: Col[float] = Field(ge=0, le=1000)
        status: Col[str] = Field(isin=["active", "inactive"])

    # Pandera validates with pandas
    pandas_df = pd.DataFrame({...})
    data_pd = Validated(pandas_df)  # Uses pandera.pandas.DataFrameSchema

    # Pandera validates with polars
    polars_df = pl.DataFrame({...})
    data_pl = Validated(polars_df)  # Uses pandera.polars.DataFrameSchema

Backend-Specific Operations
----------------------------

For backend-specific operations, access the underlying DataFrame directly:

.. code-block:: python

    orders = Orders(df)  # Works with either backend

    # Pandas-specific
    if isinstance(orders.pf_data, pd.DataFrame):
        result = orders.pf_data.groupby("customer_id").sum()

    # Polars-specific
    elif isinstance(orders.pf_data, pl.DataFrame):
        result = orders.pf_data.group_by("customer_id").sum()

Or use the ``.pf_data`` property to access backend-native methods:

.. code-block:: python

    # These work regardless of backend
    orders.pf_data  # Returns pd.DataFrame or pl.DataFrame/pl.LazyFrame

Performance Comparison
----------------------

Rough performance guidelines (results vary by dataset and operation):

+-------------------------+-------------+-------------+
| Operation               | Pandas      | Polars      |
+=========================+=============+=============+
| Small datasets (<100K)  | Similar     | Similar     |
+-------------------------+-------------+-------------+
| Medium datasets (1M)    | 1x          | 5-20x       |
+-------------------------+-------------+-------------+
| Large datasets (10M+)   | 1x          | 10-100x     |
+-------------------------+-------------+-------------+
| Memory usage            | 1x          | 0.3-0.7x    |
+-------------------------+-------------+-------------+
| Parallel aggregations   | Single core | All cores   |
+-------------------------+-------------+-------------+

**When to use Pandas:**

* Exploratory data analysis with lots of interactivity
* Working with libraries that only support Pandas
* Small to medium datasets where performance isn't critical
* When you need the extensive Pandas ecosystem

**When to use Polars:**

* Large datasets (1M+ rows)
* Performance-critical production pipelines
* Memory-constrained environments
* When you can benefit from parallel execution

Migrating Between Backends
---------------------------

Switching backends requires minimal code changes:

.. code-block:: python

    # Before (Pandas)
    df = pd.read_csv("data.csv")
    orders = Orders(df)
    result = orders.pf_data.groupby("customer_id").sum()

    # After (Polars)
    df = pl.read_csv("data.csv")
    orders = Orders(df)
    result = orders.pf_data.group_by("customer_id").sum()  # Note: group_by vs groupby

The schema definition (`Orders`) stays exactly the same. Only the DataFrame creation and backend-specific method calls change.


Adding a Backend (Advanced)
---------------------------

ProteusFrame's backend layer is an adapter interface implemented per DataFrame library.
At runtime, backend selection happens via ``proteusframe.detect_backend()`` (type-based)
or by passing ``backend=...`` explicitly.

If you want to integrate another DataFrame implementation, the intended path is:

1. Implement a ``BackendAdapter`` (see ``proteusframe.backends.base.BackendAdapter``)
2. Register it with ``proteusframe.register_backend(name, module_path)``

.. code-block:: python

        from proteusframe import register_backend

        register_backend("mybackend", "myproj.proteusframe_backends.mybackend")


Notes on cuDF
-------------

cuDF is a natural candidate because its API is intentionally close to Pandas.
That said, there are two separate concerns:

* **DataFrame operations** (get/set columns, filtering, I/O, etc.): cuDF can often be supported with a
    fairly thin adapter because many method names mirror Pandas.
* **Runtime validation** (Pandera): ProteusFrame currently relies on Pandera's Pandas and Polars backends.
    If Pandera doesn't support cuDF validation in your environment, a cuDF adapter would either need to:

    - raise a clear ``NotImplementedError`` for ``pf_validate()``, or
    - validate by materialising to Pandas (acceptable for small/medium data, but defeats GPU benefits), or
    - provide an alternative validation implementation.

If your primary goal is "typed column access + autocomplete" in production analysis code, cuDF can still be
valuable even before full runtime validation is available — but it’s best treated as an *experimental* backend
until the validation story is nailed down.
