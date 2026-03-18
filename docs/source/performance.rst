Performance
===========

FrameRight is designed to add type safety and validation with minimal performance overhead. This page documents the performance characteristics based on comprehensive benchmarks.

TL;DR
-----

* **Memory overhead**: 48 bytes per Schema instance — same whether wrapping 1,000 or 1,000,000 rows
* **Column access**: Adds ~0.2 microseconds per access (negligible for typical workloads)
* **Construction without validation**: Sub-millisecond (0.0003ms)
* **Construction with validation**: 25-51ms for 100,000 rows depending on schema complexity
* **Scaling**: Linear with data size — validation is O(n), column access is O(1)

.. note::
   These measurements are per-instance overhead (the cost of wrapping a DataFrame). There is also a one-time cost to import the FrameRight module and its dependencies (Pandera, etc.), but this is amortized across all Schema instances in your program.

Detailed Benchmarks
-------------------

All benchmarks were run on 100,000-row DataFrames unless otherwise noted.

Memory Overhead
~~~~~~~~~~~~~~~

FrameRight wraps DataFrames without copying data. Each Schema instance adds a constant 48 bytes of overhead regardless of the wrapped DataFrame size:

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 20

   * - DataFrame Size
     - Raw DataFrame
     - Schema Wrapper
     - Overhead
   * - 1,000 rows
     - 31.5 KB
     - 31.5 KB
     - 48 bytes (0.15%)
   * - 100,000 rows
     - 3.05 MB
     - 3.05 MB
     - 48 bytes (0.00%)
   * - 1,000,000 rows
     - 30.5 MB
     - 30.5 MB
     - 48 bytes (0.00%)

**Conclusion**: Per-instance memory overhead is negligible for all practical DataFrame sizes. The 48 bytes is the cost of the Python wrapper object itself (__dict__, internal state, etc.), measured using ``sys.getsizeof()``. This is separate from the one-time cost of importing the module.

Construction Time
~~~~~~~~~~~~~~~~~

Construction time depends on whether validation is enabled:

Without Validation
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    orders = OrderData(df, validate=False)  # 0.0003 ms

Wrapping a DataFrame with ``validate=False`` is essentially free — it just stores a reference.

With Validation
^^^^^^^^^^^^^^^

Validation uses Pandera under the hood and scales linearly with data size:

.. list-table::
   :header-rows: 1
   :widths: 25 25 25

   * - Rows
     - Simple Schema (3 cols)
     - Complex Schema (8 cols)
   * - 1,000
     - 2.5 ms
     - 5.1 ms
   * - 10,000
     - 2.8 ms
     - 5.6 ms
   * - 100,000
     - 13.1 ms
     - 50.8 ms

**Simple schema**: 3 columns with basic type checks (int, float, str)

**Complex schema**: 8 columns with constraints (unique, ge, isin, nullable, etc.)

**Scaling**: Approximately linear with data size. For 100k rows:

* Simple schema: 13ms (~0.13 microseconds per row)
* Complex schema: 51ms (~0.51 microseconds per row)

**Recommendation**: For performance-critical code paths that process data in small batches, consider validating once at the entry point and using ``validate=False`` for intermediate operations.

Column Access Overhead
~~~~~~~~~~~~~~~~~~~~~~

Column property access (e.g., ``orders.revenue``) goes through Python's descriptor protocol. Benchmarks show minimal overhead:

Single Column Access
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Raw DataFrame
    df['revenue']           # 9.36 microseconds

    # Schema property
    orders.revenue          # 9.59 microseconds (adds ~0.2 microseconds)

Multiple Column Access
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Raw DataFrame
    df['price'], df['qty'], df['revenue']    # 27.86 microseconds

    # Schema properties
    orders.price, orders.qty, orders.revenue # 28.73 microseconds (adds ~0.9 microseconds)

**Conclusion**: Property access adds less than 1 microsecond of overhead. For typical data pipelines where operations take milliseconds or more, this is negligible.

Column Operations
^^^^^^^^^^^^^^^^^

Once you have a Series, operations run at native speed:

.. code-block:: python

    # Both take ~55 microseconds (no measurable difference)
    df['revenue'].sum()
    orders.revenue.sum()

The overhead is in accessing the column, not in operating on it.

Polars Backend Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Polars is significantly faster than Pandas for many operations. FrameRight adds the same minimal overhead:

Construction (100k rows)
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Polars construction with validation
    orders = OrderData(pl_df)  # 5.3 ms

This is ~5x faster than Pandas (25-51ms) for the same dataset, demonstrating that Polars' performance benefits are preserved.

Column Access
^^^^^^^^^^^^^

.. code-block:: python

    # Raw Polars DataFrame
    pl_df['revenue']           # 0.43 microseconds

    # Schema property
    orders.revenue             # 0.64 microseconds (adds ~0.2 microseconds)

Polars column access is ~20x faster than Pandas, and FrameRight adds the same ~0.2 microsecond overhead.

Performance Best Practices
---------------------------

Validate at Boundaries
~~~~~~~~~~~~~~~~~~~~~~

Validate data once when it enters your system, then use ``validate=False`` for internal operations:

.. code-block:: python

    # Entry point: validate thoroughly
    def load_orders(path: str) -> OrderData:
        df = pd.read_csv(path)
        return OrderData(df, validate=True)  # Full validation

    # Internal operations: skip validation
    def process_orders(orders: OrderData) -> Revenue:
        filtered = OrderData(orders.fr_data[orders.revenue > 100], validate=False)
        # ... processing ...
        return Revenue(result, validate=False)

This gives you type safety and validation guarantees without paying the validation cost repeatedly.

Use Type Coercion Strategically
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Type coercion (``coerce=True``) adds overhead. Use it only when needed:

.. code-block:: python

    # Reading from CSV: types may not match
    df = pd.read_csv("data.csv")
    orders = OrderData(df, coerce=True)  # Convert dtypes as needed

    # Internal operations: types already correct
    result = Revenue(computed_df, validate=False)  # No coercion needed

Choose the Right Backend
~~~~~~~~~~~~~~~~~~~~~~~~

For large datasets (100k+ rows), Polars offers significant performance improvements:

.. code-block:: python

    # Pandas: ~25ms construction, ~9 microseconds column access
    import pandas as pd
    from frameright.pandas import Schema

    # Polars: ~5ms construction, ~0.4 microseconds column access
    import polars as pl
    from frameright.polars.eager import Schema

FrameRight makes switching backends trivial — the schema definition stays the same.

Use Lazy Evaluation for Complex Pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For complex data pipelines, Polars' lazy evaluation can provide significant speedups:

.. code-block:: python

    from frameright.polars.lazy import Schema, Col
    import polars as pl

    class OrderData(Schema):
        order_id: Col[int]
        revenue: Col[float]

    # Build query plan (no execution yet)
    lazy_orders = OrderData(pl.scan_csv("orders.csv"))
    filtered = lazy_orders.fr_data.filter(lazy_orders.revenue > 100)
    grouped = filtered.group_by('customer_id').agg(pl.col('revenue').sum())

    # Execute optimized plan
    result = grouped.collect()

Polars optimizes the entire query plan before execution, often resulting in significant speedups.

Summary
-------

FrameRight is designed for **zero-cost abstraction** semantics:

* **Memory**: Constant 48-byte overhead (negligible)
* **Column access**: Adds ~0.2 microseconds (negligible compared to actual operations)
* **Validation**: O(n) with data size, but can be controlled with ``validate=False``
* **Operations**: Run at native backend speed (pandas/polars/narwhals)

The type safety, IDE support, and validation features come with virtually no runtime cost for typical data pipelines where operations take milliseconds or more.

**The performance tests are available in** ``tests/test_performance.py`` **and can be run with:**

.. code-block:: bash

    pytest tests/test_performance.py -v

This will show detailed timing and memory measurements on your system.
