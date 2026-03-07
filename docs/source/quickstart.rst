Quick Start
===========

Installation
------------

.. code-block:: bash

    # For Pandas backend (default)
    pip install proteusframe

    # For Polars backend (optional)
    pip install proteusframe[polars]

ProteusFrame automatically detects which backend you're using based on the DataFrame type.
No configuration needed.


Defining a Schema
-----------------

Define your DataFrame schema as a Python class using ``Col[T]`` type hints:

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

**With Pandas:**

.. code-block:: python

    # From a DataFrame
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

**With Polars:**

.. code-block:: python

    import polars as pl

    # From a Polars DataFrame
    df = pl.DataFrame({...})
    customers = Customer(df)  # Automatically uses Polars backend

    # From a CSV file
    df = pl.read_csv("customers.csv")
    customers = Customer(df)

The same schema class works with both backends. Backend detection is automatic.


Type-Safe Access
----------------

.. code-block:: python

    # IDE autocomplete works on all columns
    print(customers.name)
    print(customers.age.mean())

    # Filter with type safety
    young = customers.pf_filter(customers.age < 30)


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

**Benefits of Pandera integration:**

* Industry-standard validation library with extensive testing
* Clear, actionable error messages with row/column context
* Works with both Pandas and Polars backends
* Extensible — access the underlying Pandera schema for custom checks


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
