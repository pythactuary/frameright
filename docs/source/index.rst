FrameRight Documentation
========================

Type-safe DataFrame wrapper with runtime validation for production data pipelines.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   backends
   performance
   examples
   api

What is FrameRight?
-------------------

FrameRight provides a Pydantic-like interface for Pandas and Polars DataFrames, enabling:

* **Type-aware column access** with IDE autocomplete
* **Multi-backend support** — works seamlessly with Pandas and Polars
* **Production-grade validation** — powered by Pandera for runtime schema checking
* **Self-documenting** data structures with docstrings and type hints
* **Reduced bugs** in data pipelines through static type checking
* **Native performance** — direct vectorized operations on the underlying backend

Quick Example
-------------

**With Pandas:**

.. code-block:: python

    from frameright import Schema, Field
    from frameright.typing import Col
    from typing import Optional
    import pandas as pd

    class Orders(Schema):
        item_price: Col[float]
        """The price per unit of the item."""
        quantity_sold: Col[int] = Field(ge=0)
        """Number of units sold."""
        revenue: Optional[Col[float]]

    df = pd.DataFrame({...})
    orders = Orders(df)  # Validates with Pandera
    orders.revenue = orders.item_price * orders.quantity_sold

**With Polars:**

.. code-block:: python

    import polars as pl

    # Same schema definition works for both backends
    df = pl.DataFrame({...})
    orders = Orders(df)  # Automatically uses Polars backend
    orders.revenue = orders.item_price * orders.quantity_sold

Key Benefits
------------

* **IDE-first design**: Full autocomplete, hover docs, and static error checking
* **Powered by Pandera**: Uses the industry-standard validation library for bulletproof runtime checks
* **Backend-agnostic schemas**: Write once, use with Pandas or Polars
* **Minimal performance overhead**: Adds only ~0.2 microseconds per column access and 48 bytes of memory

Performance
-----------

FrameRight is designed for negligible overhead:

* **Memory**: 48 bytes per Schema instance (same for 1k or 1M rows)
* **Column access**: Adds ~0.2 microseconds per property access
* **Construction**: Sub-millisecond without validation, 25-51ms for 100k rows with validation
* **Operations**: Run at native backend speed (pandas/polars/narwhals)

See the :doc:`performance` page for detailed benchmarks and best practices.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
