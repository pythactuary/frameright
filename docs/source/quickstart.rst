Quick Start
===========

Installation
------------

.. code-block:: bash

    # For Pandas backend (default)
    pip install frameright

    # For Polars backend (optional)
    pip install frameright[polars]

    # For Narwhals backend (optional)
    pip install frameright[narwhals]

FrameRight supports multiple backends. Use backend-specific classes for type safety,
by importing from the appropriate backend module (``frameright.pandas``, ``frameright.polars.eager``, etc.).


Defining a Schema
-----------------

Define your DataFrame schema as a Python class using ``Col[T]`` type hints:

.. note::

    For the best editor experience, import backend-specific typing shims:

    * Pandas: ``from frameright.typing.pandas import Col``
    * Polars eager: ``from frameright.typing.polars_eager import Col``
    * Polars lazy: ``from frameright.typing.polars_lazy import Col``

    The generic ``from frameright.typing import Col`` also works and preserves the
    inner type parameter ``T`` for schema annotations.

    **Important typing note:** Pandas has mature type stubs, so type checkers can often
    treat attribute accessors like ``obj.amount`` as ``pd.Series[float]``.
    Polars and Narwhals do not currently expose fully generic ``Series[T]`` / ``Expr[T]``
    types upstream, so type checkers typically see ``pl.Series`` / ``pl.Expr`` / ``nw.Series``
    (inner type is best-effort today).

.. code-block:: python

    from frameright import Schema, Field
    from frameright.typing import Col
    from typing import Optional
    import pandas as pd

    class Customer(Schema):
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

**With Pandas (using base Schema):**

.. code-block:: python

    # From a pandas DataFrame
    df = pd.DataFrame({...})
    customers = Customer(df)

    # Load from CSV and wrap
    df = pd.read_csv("customers.csv")
    customers = Customer(df)

**With Polars (eager - recommended for interactive use):**

.. code-block:: python

    import polars as pl
    from frameright.polars.eager import Schema, Col, Field

    # Define schema using polars eager module
    class Customer(Schema):  # Uses Polars eager backend
        customer_id: Col[int] = Field(unique=True)
        name: Col[str]
        ...

    # From a Polars DataFrame
    df = pl.DataFrame({...})
    customers = Customer(df)  # Uses Polars eager backend

**All backends work the same way:**

.. code-block:: python

    # Pandas
    from frameright.pandas import Schema, Col, Field
    
    # Polars eager
    from frameright.polars.eager import Schema, Col, Field
    
    # Polars lazy
    from frameright.polars.lazy import Schema, Col, Field
    
    # Narwhals (backend-agnostic)
    from frameright.narwhals.eager import Schema, Col, Field

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
    young_df = customers.fr_data[customers.age < 30]
    young = Customer(young_df, validate=False)


Validation (Powered by Pandera)
--------------------------------

FrameRight uses **Pandera** for runtime validation, giving you production-tested constraint
checking with helpful error messages.

Validation runs automatically on construction:

.. code-block:: python

    customers = Customer(df)  # Validates schema and constraints

You can also run validation manually:

.. code-block:: python

    customers.fr_validate()

To skip validation (e.g. after filtering):

.. code-block:: python

    customers = Customer(df, validate=False)

.. tip::

    In production pipelines, a common pattern is to validate at I/O boundaries (CSV reads, API inputs)
    and at team handoffs (function outputs), while skipping validation on intermediate steps for speed.
    You can always call ``obj.fr_validate()`` right before returning a Schema from a public API.

**Benefits of Pandera integration:**

* Industry-standard validation library with extensive testing
* Clear, actionable error messages with row/column context
* Works with both Pandas and Polars backends
* Extensible — use Pandera directly for custom checks on ``obj.fr_data``


Type Coercion
-------------

When loading messy data (e.g. CSV where everything is a string):

.. code-block:: python

    messy_df = pd.read_csv("data.csv")
    customers = Customer(messy_df, coerce=True)


Schema Introspection
--------------------

.. code-block:: python

    for col in Customer.fr_schema_info():
        print(col["attribute"], col["type"], col["required"])
    # customer_id  int  True
    # ...
