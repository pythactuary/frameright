Quick Start
===========

Installation
------------

.. code-block:: bash

    # For Pandas backend (default)
    pip install proteusframe

    # For Polars backend (optional)
    pip install proteusframe[polars]

    # For Narwhals backend (optional)
    pip install proteusframe[narwhals]

ProteusFrame supports multiple backends. Use backend-specific classes for type safety,
or use the base ``ProteusFrame`` class which defaults to pandas.


Defining a Schema
-----------------

Define your DataFrame schema as a Python class using ``Col[T]`` type hints:

.. note::

    For the best editor experience, import backend-specific typing shims:

    * Pandas: ``from proteusframe.typing.pandas import Col, Index``
    * Polars eager: ``from proteusframe.typing.polars_eager import Col, Index``
    * Polars lazy: ``from proteusframe.typing.polars_lazy import Col, Index``

    The generic ``from proteusframe.typing import Col`` also works and preserves the
    inner type parameter ``T`` for schema annotations.

    **Important typing note:** Pandas has mature type stubs, so type checkers can often
    treat attribute accessors like ``obj.amount`` as ``pd.Series[float]``.
    Polars and Narwhals do not currently expose fully generic ``Series[T]`` / ``Expr[T]``
    types upstream, so type checkers typically see ``pl.Series`` / ``pl.Expr`` / ``nw.Series``
    (inner type is best-effort today).

.. code-block:: python

    from proteusframe import ProteusFrame, Field
    from proteusframe.typing import Col
    from typing import Optional
    import pandas as pd

    class Customer(ProteusFrame):
        customer_id: Col[int] = Field(unique=True, nullable=False)
        """Unique customer identifier."""
        name: Col[str] = Field(min_length=1)
        """Customer's full name."""
        age: Col[int] = Field(ge=18, le=120)
        """Customer's age in years."""
        email: Col[str] = Field(regex=r'^[\w\.\-]+@[\w\.\-]+\.\w+$')
        """Contact email address."""
        lifetime_value: Optional[Col[float]]
        """Total spend (optional)."""


Loading Data
------------

**With Pandas (using base ProteusFrame):**

.. code-block:: python

    # From a DataFrame (defaults to pandas)
    df = pd.DataFrame({...})
    customers = Customer(df)

    # From a CSV file
    customers = Customer.pf_from_csv("customers.csv")

    # From a dictionary
    customers = Customer.pf_from_dict({
        "customer_id": [1, 2],
        "name": ["Alice", "Bob"],
        ...
    })

**With Polars (recommended: use ProteusFramePolars):**

.. code-block:: python

    import polars as pl
    from proteusframe import ProteusFramePolars, Field
    from proteusframe.typing.polars_eager import Col

    # Define schema with backend-specific class
    class Customer(ProteusFramePolars):  # Explicitly uses Polars
        customer_id: Col[int] = Field(unique=True)
        name: Col[str]
        ...

    # From a Polars DataFrame
    df = pl.DataFrame({...})
    customers = Customer(df)  # Uses Polars backend

**Alternative: Use base ProteusFrame with backend parameter:**

.. code-block:: python

    from proteusframe import ProteusFrame

    class Customer(ProteusFrame):  # Defaults to pandas
        ...

    # Explicitly specify polars
    df = pl.DataFrame({...})
    customers = Customer(df, backend="polars")


Type-Safe Access
----------------

.. code-block:: python

    # IDE autocomplete works on all columns
    print(customers.name)
    print(customers.age.mean())

    # Filter using the backend's native API, then re-wrap
    young_df = customers.pf_data[customers.age < 30]
    young = Customer(young_df, validate=False)


Validation (Powered by Pandera)
--------------------------------

ProteusFrame uses **Pandera** for runtime validation, giving you production-tested constraint
checking with helpful error messages.

Validation runs automatically on construction:

.. code-block:: python

    customers = Customer(df)  # Validates schema and constraints

You can also run validation manually:

.. code-block:: python

    customers.pf_validate()

To skip validation (e.g. after filtering):

.. code-block:: python

    customers = Customer(df, validate=False)

.. tip::

    In production pipelines, a common pattern is to validate at I/O boundaries (CSV reads, API inputs)
    and at team handoffs (function outputs), while skipping validation on intermediate steps for speed.
    You can always call ``obj.pf_validate()`` right before returning a ProteusFrame from a public API.

**Benefits of Pandera integration:**

* Industry-standard validation library with extensive testing
* Clear, actionable error messages with row/column context
* Works with both Pandas and Polars backends
* Extensible — use Pandera directly for custom checks on ``obj.pf_data``


Type Coercion
-------------

When loading messy data (e.g. CSV where everything is a string):

.. code-block:: python

    messy_df = pd.read_csv("data.csv")
    customers = Customer.pf_coerce(messy_df)


Schema Introspection
--------------------

.. code-block:: python

    for col in Customer.pf_schema_info():
        print(col["attribute"], col["type"], col["required"])
    # customer_id  int  True
    # ...
