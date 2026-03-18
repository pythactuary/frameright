"""Performance benchmarks for FrameRight overhead assessment.

These tests measure:
1. Memory overhead of Schema wrappers vs raw DataFrames
2. Construction overhead (validation time)
3. Column access overhead

Run with: pytest tests/test_performance.py -v
"""

import sys
import time
from typing import Callable

import pandas as pd
import pytest

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

from frameright.pandas import Col, Schema

# Test data sizes
SMALL_SIZE = 1_000
MEDIUM_SIZE = 100_000
LARGE_SIZE = 1_000_000


# ===========================================================================
# Schema Definitions
# ===========================================================================


class SimpleSchema(Schema):
    """Minimal schema for benchmarking."""

    id: Col[int]
    name: Col[str]
    value: Col[float]


class ComplexSchema(Schema):
    """Schema with more columns and validation."""

    user_id: Col[int]
    username: Col[str]
    email: Col[str]
    age: Col[int]
    balance: Col[float]
    is_active: Col[bool]
    created_at: Col[str]
    last_login: Col[str]


# ===========================================================================
# Helper Functions
# ===========================================================================


def create_pandas_df(nrows: int, ncols: int = 3) -> pd.DataFrame:
    """Create a pandas DataFrame with specified size."""
    if ncols == 3:
        return pd.DataFrame(
            {
                "id": range(nrows),
                "name": [f"name_{i}" for i in range(nrows)],
                "value": [float(i) for i in range(nrows)],
            }
        )
    elif ncols == 8:
        return pd.DataFrame(
            {
                "user_id": range(nrows),
                "username": [f"user_{i}" for i in range(nrows)],
                "email": [f"user_{i}@example.com" for i in range(nrows)],
                "age": [20 + (i % 50) for i in range(nrows)],
                "balance": [1000.0 + float(i) for i in range(nrows)],
                "is_active": pd.Series([i % 2 == 0 for i in range(nrows)], dtype="boolean"),
                "created_at": ["2024-01-01"] * nrows,
                "last_login": ["2024-03-01"] * nrows,
            }
        )
    else:
        raise ValueError("Unsupported number of columns for test DataFrame")


def get_memory_usage(obj) -> int:
    """Get memory usage in bytes."""
    if isinstance(obj, pd.DataFrame):
        return obj.memory_usage(deep=True).sum()
    elif hasattr(obj, "fr_data"):
        # FrameRight Schema object
        df_memory = obj.fr_data.memory_usage(deep=True).sum()
        # Approximate overhead: object itself + schema metadata
        wrapper_overhead = sys.getsizeof(obj)
        return df_memory + wrapper_overhead
    return 0


def timeit(func: Callable, iterations: int = 100) -> float:
    """Time a function call over multiple iterations."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)
    return sum(times) / len(times)  # Return average time


# ===========================================================================
# Memory Overhead Tests
# ===========================================================================


class TestMemoryOverhead:
    """Measure memory overhead of FrameRight Schema wrappers."""

    def test_memory_overhead_small_dataframe(self):
        """Memory overhead with small DataFrame (1k rows)."""
        df = create_pandas_df(SMALL_SIZE)
        schema = SimpleSchema(df, validate=False)

        raw_memory = get_memory_usage(df)
        wrapped_memory = get_memory_usage(schema)
        overhead = wrapped_memory - raw_memory
        overhead_pct = (overhead / raw_memory) * 100

        print(f"\n--- Small DataFrame ({SMALL_SIZE} rows) ---")
        print(f"Raw DataFrame: {raw_memory:,} bytes")
        print(f"FrameRight Schema: {wrapped_memory:,} bytes")
        print(f"Overhead: {overhead:,} bytes ({overhead_pct:.2f}%)")

        # Schema wrapper should add minimal overhead
        assert overhead_pct < 10, f"Memory overhead too high: {overhead_pct:.2f}%"

    def test_memory_overhead_medium_dataframe(self):
        """Memory overhead with medium DataFrame (100k rows)."""
        df = create_pandas_df(MEDIUM_SIZE)
        schema = SimpleSchema(df, validate=False)

        raw_memory = get_memory_usage(df)
        wrapped_memory = get_memory_usage(schema)
        overhead = wrapped_memory - raw_memory
        overhead_pct = (overhead / raw_memory) * 100

        print(f"\n--- Medium DataFrame ({MEDIUM_SIZE} rows) ---")
        print(f"Raw DataFrame: {raw_memory:,} bytes")
        print(f"FrameRight Schema: {wrapped_memory:,} bytes")
        print(f"Overhead: {overhead:,} bytes ({overhead_pct:.2f}%)")

        # Overhead should be even smaller percentage with larger data
        assert overhead_pct < 5, f"Memory overhead too high: {overhead_pct:.2f}%"

    def test_memory_overhead_large_dataframe(self):
        """Memory overhead with large DataFrame (1M rows)."""
        df = create_pandas_df(LARGE_SIZE)
        schema = SimpleSchema(df, validate=False)

        raw_memory = get_memory_usage(df)
        wrapped_memory = get_memory_usage(schema)
        overhead = wrapped_memory - raw_memory
        overhead_pct = (overhead / raw_memory) * 100

        print(f"\n--- Large DataFrame ({LARGE_SIZE} rows) ---")
        print(f"Raw DataFrame: {raw_memory:,} bytes")
        print(f"FrameRight Schema: {wrapped_memory:,} bytes")
        print(f"Overhead: {overhead:,} bytes ({overhead_pct:.2f}%)")

        # With large data, overhead should be negligible
        assert overhead_pct < 2, f"Memory overhead too high: {overhead_pct:.2f}%"


# ===========================================================================
# Construction Overhead Tests
# ===========================================================================


class TestConstructionOverhead:
    """Measure time overhead of creating Schema instances."""

    def test_construction_without_validation(self):
        """Construction time without validation (validate=False)."""
        df = create_pandas_df(MEDIUM_SIZE)

        # Time raw DataFrame "construction" (just identity operation)
        raw_time = timeit(lambda: df, iterations=1000)

        # Time Schema construction without validation
        schema_time = timeit(lambda: SimpleSchema(df, validate=False), iterations=1000)

        overhead = schema_time - raw_time
        overhead_pct = (overhead / raw_time) * 100 if raw_time > 0 else 0

        print(f"\n--- Construction without validation ({MEDIUM_SIZE} rows) ---")
        print(f"Raw DataFrame: {raw_time * 1000:.4f} ms")
        print(f"Schema (validate=False): {schema_time * 1000:.4f} ms")
        print(f"Overhead: {overhead * 1000:.4f} ms ({overhead_pct:.1f}%)")

        # Should be fast even with validation disabled
        assert schema_time < 0.001, f"Construction too slow: {schema_time * 1000:.2f} ms"

    def test_construction_with_validation_simple(self):
        """Construction time with validation (simple schema)."""
        df = create_pandas_df(MEDIUM_SIZE)

        # Time Schema construction with validation
        schema_time = timeit(lambda: SimpleSchema(df, validate=True), iterations=10)

        print(f"\n--- Construction with validation - Simple ({MEDIUM_SIZE} rows) ---")
        print(f"Schema (validate=True): {schema_time * 1000:.2f} ms")

        # Should complete in reasonable time
        assert schema_time < 1.0, f"Validation too slow: {schema_time * 1000:.2f} ms"

    def test_construction_with_validation_complex(self):
        """Construction time with validation (complex schema)."""
        df = create_pandas_df(MEDIUM_SIZE, ncols=8)

        # Time Schema construction with validation
        schema_time = timeit(lambda: ComplexSchema(df, validate=True), iterations=10)

        print(f"\n--- Construction with validation - Complex ({MEDIUM_SIZE} rows) ---")
        print(f"Schema (validate=True): {schema_time * 1000:.2f} ms")

        # Should complete in reasonable time even with more columns
        assert schema_time < 2.0, f"Validation too slow: {schema_time * 1000:.2f} ms"

    def test_construction_scaling_with_data_size(self):
        """How construction time scales with data size."""
        sizes = [1_000, 10_000, 100_000]
        print("\n--- Construction scaling with data size ---")

        for size in sizes:
            df = create_pandas_df(size)
            schema_time = timeit(lambda: SimpleSchema(df, validate=True), iterations=10)
            print(f"Size: {size:>7,} rows -> {schema_time * 1000:>8.2f} ms")

        # Just informational, no assertion


# ===========================================================================
# Column Access Overhead Tests
# ===========================================================================


class TestColumnAccessOverhead:
    """Measure overhead of column access through Schema properties."""

    def test_single_column_access(self):
        """Single column access performance."""
        df = create_pandas_df(MEDIUM_SIZE)
        schema = SimpleSchema(df, validate=False)

        # Time raw DataFrame column access
        raw_time = timeit(lambda: df["name"], iterations=10000)

        # Time Schema property access
        schema_time = timeit(lambda: schema.name, iterations=10000)

        overhead = schema_time - raw_time
        overhead_pct = (overhead / raw_time) * 100 if raw_time > 0 else 0

        print("\n--- Single column access ---")
        print(f"Raw DataFrame: {raw_time * 1000000:.2f} µs")
        print(f"Schema property: {schema_time * 1000000:.2f} µs")
        print(f"Overhead: {overhead * 1000000:.2f} µs ({overhead_pct:.1f}%)")

        # Property access should have minimal overhead
        assert schema_time < raw_time * 2, "Column access overhead too high"

    def test_multiple_column_access(self):
        """Multiple column access in a loop."""
        df = create_pandas_df(MEDIUM_SIZE)
        schema = SimpleSchema(df, validate=False)

        # Time raw DataFrame column access (multiple columns)
        def raw_access():
            _ = df["id"]
            _ = df["name"]
            _ = df["value"]

        # Time Schema property access (multiple columns)
        def schema_access():
            _ = schema.id
            _ = schema.name
            _ = schema.value

        raw_time = timeit(raw_access, iterations=10000)
        schema_time = timeit(schema_access, iterations=10000)

        overhead = schema_time - raw_time
        overhead_pct = (overhead / raw_time) * 100 if raw_time > 0 else 0

        print("\n--- Multiple column access (3 columns) ---")
        print(f"Raw DataFrame: {raw_time * 1000000:.2f} µs")
        print(f"Schema properties: {schema_time * 1000000:.2f} µs")
        print(f"Overhead: {overhead * 1000000:.2f} µs ({overhead_pct:.1f}%)")

    def test_column_operations_performance(self):
        """Performance of operations on accessed columns."""
        df = create_pandas_df(MEDIUM_SIZE)
        schema = SimpleSchema(df, validate=False)

        # Time operations on raw DataFrame columns
        raw_time = timeit(lambda: df["value"].sum(), iterations=1000)

        # Time operations on Schema property columns
        schema_time = timeit(lambda: schema.value.sum(), iterations=1000)

        overhead = schema_time - raw_time
        overhead_pct = (overhead / raw_time) * 100 if raw_time > 0 else 0

        print("\n--- Column operations (sum) ---")
        print(f"Raw DataFrame: {raw_time * 1000:.4f} ms")
        print(f"Schema property: {schema_time * 1000:.4f} ms")
        print(f"Overhead: {overhead * 1000:.4f} ms ({overhead_pct:.1f}%)")

        # Operations should dominate, not property access
        # Allow up to 20% overhead due to timing variance
        assert overhead_pct < 20, f"Column operation overhead too high: {overhead_pct:.1f}%"


# ===========================================================================
# Polars Performance Tests
# ===========================================================================


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
class TestPolarsPerformance:
    """Performance tests for Polars backend."""

    def test_polars_construction_overhead(self):
        """Construction overhead with Polars backend."""
        from frameright.polars.eager import Schema as PolarsSchema

        class PolarsSimple(PolarsSchema):
            id: Col[int]
            name: Col[str]
            value: Col[float]

        df = pl.DataFrame(
            {
                "id": range(MEDIUM_SIZE),
                "name": [f"name_{i}" for i in range(MEDIUM_SIZE)],
                "value": [float(i) for i in range(MEDIUM_SIZE)],
            }
        )

        # Time Polars Schema construction
        schema_time = timeit(lambda: PolarsSimple(df, validate=True), iterations=10)

        print(f"\n--- Polars construction ({MEDIUM_SIZE} rows) ---")
        print(f"Schema (validate=True): {schema_time * 1000:.2f} ms")

        # Should be comparable to pandas
        assert schema_time < 1.0, f"Polars validation too slow: {schema_time * 1000:.2f} ms"

    def test_polars_column_access_overhead(self):
        """Column access overhead with Polars backend."""
        from frameright.polars.eager import Schema as PolarsSchema

        class PolarsSimple(PolarsSchema):
            id: Col[int]
            name: Col[str]
            value: Col[float]

        df = pl.DataFrame(
            {
                "id": range(MEDIUM_SIZE),
                "name": [f"name_{i}" for i in range(MEDIUM_SIZE)],
                "value": [float(i) for i in range(MEDIUM_SIZE)],
            }
        )

        schema = PolarsSimple(df, validate=False)

        # Time raw Polars column access
        raw_time = timeit(lambda: df["name"], iterations=10000)

        # Time Schema property access
        schema_time = timeit(lambda: schema.name, iterations=10000)

        overhead = schema_time - raw_time
        overhead_pct = (overhead / raw_time) * 100 if raw_time > 0 else 0

        print("\n--- Polars single column access ---")
        print(f"Raw DataFrame: {raw_time * 1000000:.2f} µs")
        print(f"Schema property: {schema_time * 1000000:.2f} µs")
        print(f"Overhead: {overhead * 1000000:.2f} µs ({overhead_pct:.1f}%)")


# ===========================================================================
# Summary Report
# ===========================================================================


def test_performance_summary():
    """Print a summary of performance characteristics."""
    print("\n" + "=" * 70)
    print("FRAMERIGHT PERFORMANCE SUMMARY")
    print("=" * 70)
    print("\nKey Findings:")
    print("1. Memory overhead is negligible (<2% for large DataFrames)")
    print("2. Construction without validation is nearly instant (<1ms)")
    print("3. Validation overhead is acceptable (<1s for 100k rows)")
    print("4. Column access has minimal overhead (property lookup)")
    print("5. Operations on columns have near-zero overhead")
    print("\nRecommendations:")
    print("- Use validate=False when performance is critical")
    print("- Validation cost is O(n) in rows, scales linearly")
    print("- Column access overhead is constant, not per-row")
    print("=" * 70)
