StructFrame Documentation
=========================

Type-safe DataFrame wrapper with runtime validation for production data pipelines.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   api
   examples

What is StructFrame?
--------------------

StructFrame provides a Pydantic-like interface for pandas DataFrames, enabling:

* **Type-safe column access** with IDE autocomplete
* **Runtime validation** of schemas and constraints
* **Self-documenting** data structures
* **Reduced bugs** in data pipelines

Quick Example
-------------

.. code-block:: python

    from structframe import StructFrame, Field
    from structframe.typing import Col
    from typing import Optional
    import pandas as pd

    class Orders(StructFrame):
        item_price: Col[float]
        """The price per unit of the item."""
        quantity_sold: Col[int] = Field(ge=0)
        """Number of units sold."""
        revenue: Optional[Col[float]]

    orders = Orders(df)
    orders.revenue = orders.item_price * orders.quantity_sold


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
