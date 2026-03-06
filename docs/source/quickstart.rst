Quick Start
===========

Installation
------------

.. code-block:: bash

    pip install structframe


Defining a Schema
-----------------

Define your DataFrame schema as a Python class using ``Col[T]`` type hints:

.. code-block:: python

    from structframe import StructFrame, Field
    from structframe.typing import Col
    from typing import Optional
    import pandas as pd

    class Customer(StructFrame):
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

.. code-block:: python

    # From a DataFrame
    df = pd.DataFrame({...})
    customers = Customer(df)

    # From a CSV file
    customers = Customer.sf_from_csv("customers.csv")

    # From a dictionary
    customers = Customer.sf_from_dict({
        "customer_id": [1, 2],
        "name": ["Alice", "Bob"],
        ...
    })


Type-Safe Access
----------------

.. code-block:: python

    # IDE autocomplete works on all columns
    print(customers.name)
    print(customers.age.mean())

    # Filter with type safety
    young = customers.sf_filter(customers.age < 30)


Validation
----------

Validation runs automatically on construction. You can also run it manually:

.. code-block:: python

    customers.sf_validate()

To skip validation (e.g. after filtering):

.. code-block:: python

    customers = Customer(df, validate=False)


Type Coercion
-------------

When loading messy data (e.g. CSV where everything is a string):

.. code-block:: python

    messy_df = pd.read_csv("data.csv")
    customers = Customer.sf_coerce(messy_df)


Schema Introspection
--------------------

.. code-block:: python

    print(Customer.sf_schema_info())
    #   attribute       column  type  required  nullable  unique  constraints  description
    # 0 customer_id  customer_id   int      True     False    True         None  ...
    # ...
