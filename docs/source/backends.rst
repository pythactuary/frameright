Backend Support
===============

FrameRight supports multiple DataFrame backends. You can choose your backend using:

1. **Backend-specific classes** (recommended for type safety)
2. **Base Schema class** (defaults to pandas, or specify ``backend`` parameter)

Supported Backends
------------------

* **Pandas** — Mature ecosystem, extensive third-party library support
* **Polars** — High-performance, Rust-based, with lazy evaluation
* **Narwhals** — Backend-agnostic DataFrame API

Backend Selection
-----------------

**Explicit Module Imports (Required)**

Import ``Schema`` and ``Col`` from backend-specific modules:

.. code-block:: python

    from frameright.pandas import Schema as PandasSchema, Col, Field
    from frameright.polars.eager import Schema as PolarsSchema, Col as PolarsCol, Field
    from frameright.polars.lazy import Schema as LazySchema, Col as LazyCol, Field

    # Pandas backend
    class OrdersPandas(PandasSchema):
        order_id: Col[int] = Field(unique=True)
        revenue: Col[float] = Field(ge=0)

    # Polars eager backend
    class OrdersPolars(PolarsSchema):
        order_id: PolarsCol[int] = Field(unique=True)
        revenue: PolarsCol[float] = Field(ge=0)

    # Polars lazy backend
    class OrdersLazy(LazySchema):
        order_id: LazyCol[int] = Field(unique=True)
        revenue: LazyCol[float] = Field(ge=0)

**Backend Auto-Detection**

Each backend module's ``Schema`` class is tied to its specific backend.
The underlying data type determines validation behavior (e.g., ``pl.DataFrame`` uses ``pandera.polars``):

.. code-block:: python

    from frameright.polars.eager import Schema, Col, Field
    from frameright.typing import Col

    class Orders(Schema):  # Defaults to pandas
        order_id: Col[int] = Field(unique=True)
        revenue: Col[float] = Field(ge=0)

    # Pandas backend (default)
    import pandas as pd
    pandas_df = pd.DataFrame({...})
    orders_pd = Orders(pandas_df)  # Uses pandas backend

    # Polars backend (explicit parameter)
    import polars as pl
    polars_df = pl.DataFrame({...})
    orders_pl = Orders(polars_df, backend="polars")  # Explicitly use polars

**Type Safety:** Explicit module imports like ``frameright.pandas``, ``frameright.polars.eager`` provide stronger
type guarantees and are recommended for production code.

Typing Notes
------------

FrameRight schemas are backend-agnostic, but you can opt into backend-specific typing for a better IDE experience:

* Pandas: ``from frameright.typing.pandas import Col`` (columns can type-check as ``pd.Series[T]`` with pandas stubs)
* Polars eager: ``from frameright.typing.polars_eager import Col`` (columns type-check as ``pl.Series``)
* Polars lazy: ``from frameright.typing.polars_lazy import Col`` (columns type-check as ``pl.Expr`` for expression chaining)

.. note::

    Polars and Narwhals do not currently expose fully generic ``Series[T]`` / ``Expr[T]`` types upstream.
    FrameRight's ``Col[T]`` is still valuable as a schema contract and for IDE autocomplete, but type checkers
    generally treat the runtime values as unparameterized ``pl.Series`` / ``pl.Expr`` / ``nw.Series`` today.

At runtime, the actual values you get depend on the backend:

* Pandas: properties return ``pd.Series``
* Polars eager ``pl.DataFrame``: properties return ``pl.Series``
* Polars lazy ``pl.LazyFrame``: properties return ``pl.Expr`` (lazy expressions)

Python 3.12+ Generic Syntax (PEP 695)
------------------------------------

Python 3.12 adds a new generic class syntax that avoids manual ``TypeVar`` boilerplate.
FrameRight works well with this style and still preserves backend inference:

.. code-block:: python

        from frameright import Schema, Field
        from frameright.typing import Col

        class Sales[T](Schema[T]):
                order_id: Col[int] = Field(unique=True)
                customer: Col[str]
                revenue: Col[float] = Field(ge=0)

Why both ``T``s?

* ``Sales[T]`` declares the generic parameter.
* ``Schema[T]`` forwards it to the base class so type checkers can infer
    the backend type from the constructor argument.

This is the shortest syntax that keeps full static typing without defaulting
to a specific backend or collapsing to ``Any``.

Pandas Backend
--------------

**Installation:**

.. code-block:: bash

    pip install frameright

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

    # Use fr_data for backend-specific operations
    customer_totals = orders.fr_data.groupby(orders.customer_id).sum()

    # You can always access the underlying DataFrame directly
    print(orders.fr_data.columns)

Polars Backend
--------------

**Installation:**

.. code-block:: bash

    pip install frameright[polars]

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

    # Use fr_data for backend-specific operations
    customer_totals = orders.fr_data.group_by(orders.customer_id).sum()

    # You can always access the underlying DataFrame directly
    print(orders.fr_data.columns)

**Lazy Evaluation:**

Polars supports lazy evaluation for complex query optimization:

.. code-block:: python

    # LazyFrame is automatically handled
    lazy_df = pl.scan_csv("data.csv")
    orders = Orders(lazy_df)  # Schema works with LazyFrames too

    # Operations are lazy until you collect()
    filtered_df = orders.fr_data.filter(orders.revenue > 1000)
    # Execute the full query plan
    result = filtered_df.collect()

Backend-Agnostic Schemas
-------------------------

The key benefit: **write your schema once, use it with any backend**.

.. code-block:: python

    class SalesData(Schema):
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

    class Validated(Schema):
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

For backend-specific operations, use ``fr_data`` to access the underlying DataFrame directly:

.. code-block:: python

    orders = Orders(df)  # Works with either backend

    # Pandas-specific
    if isinstance(orders.fr_data, pd.DataFrame):
        result = orders.fr_data.groupby(orders.customer_id).sum()

    # Polars-specific
    elif isinstance(orders.fr_data, pl.DataFrame):
        result = orders.fr_data.group_by(orders.customer_id).sum()

For backend-agnostic access, use ``fr_data`` which returns a `narwhals
<https://narwhals-dev.github.io/narwhals/>`__ wrapper with full IDE autocomplete:

.. code-block:: python

    import narwhals as nw

    # These work regardless of backend
    orders.fr_data              # Returns nw.DataFrame or nw.LazyFrame
    orders.fr_data.columns      # Column names (backend-agnostic)
    orders.fr_data.schema       # Narwhals schema
    orders.fr_data.to_native()  # Escape to native DataFrame (zero-copy)

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
    result = orders.fr_data.groupby(orders.customer_id).sum()

    # After (Polars)
    df = pl.read_csv("data.csv")
    orders = Orders(df)
    result = orders.fr_data.group_by(orders.customer_id).sum()  # Note: group_by vs groupby

The schema definition (``Orders``) stays exactly the same. Only the DataFrame creation and backend-specific method calls change.
For backend-agnostic code, use ``fr_data`` — the narwhals API is the same regardless of backend.


Adding a Backend (Advanced)
---------------------------

FrameRight's backend layer is a simple adapter interface implemented per DataFrame library.
Each backend module (``frameright.pandas``, ``frameright.polars.eager``, etc.) provides its own
``Schema`` class with a hardcoded backend adapter instance.

**No auto-detection or dispatch logic** — importing from a specific module gives you that backend.
This design is intentionally simple and fast:

.. code-block:: python

    from frameright.pandas import Schema       # _fr_backend = PandasBackend()
    from frameright.polars.eager import Schema # _fr_backend = PolarsEagerBackend()
    from frameright.polars.lazy import Schema  # _fr_backend = PolarsLazyBackend()

If you want to integrate another DataFrame implementation:

1. Implement a ``BackendAdapter`` (see ``frameright.backends.base.BackendAdapter``)
2. Create a new module with a ``Schema`` class that sets ``_fr_backend``

.. code-block:: python

    from frameright.backends.base import BackendAdapter
    from frameright.core import BaseSchema

    class MyBackend(BackendAdapter):
        # Implement required methods...
        pass

    class Schema(BaseSchema):
        _fr_backend = MyBackend()

    # Users import your Schema directly
    from mypackage import Schema


Notes on cuDF
-------------

cuDF is a natural candidate because its API is intentionally close to Pandas.
That said, there are two separate concerns:

* **DataFrame operations** (get/set columns, filtering, I/O, etc.): cuDF can often be supported with a
    fairly thin adapter because many method names mirror Pandas.
* **Runtime validation** (Pandera): Schema currently relies on Pandera's Pandas and Polars backends.
    If Pandera doesn't support cuDF validation in your environment, a cuDF adapter would either need to:

    - raise a clear ``NotImplementedError`` for ``fr_validate()``, or
    - validate by materialising to Pandas (acceptable for small/medium data, but defeats GPU benefits), or
    - provide an alternative validation implementation.

If your primary goal is "typed column access + autocomplete" in production analysis code, cuDF can still be
valuable even before full runtime validation is available — but it’s best treated as an *experimental* backend
until the validation story is nailed down.
