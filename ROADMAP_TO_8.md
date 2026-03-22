# Roadmap to 8/10 Library Score

**Current Score: 7.5/10** (now 7.9/10 with Python 3.9+ support added)  
**Target Score: 8.0/10**

## Core Philosophy: ODM, Not DataFrame Abstraction

**Schema is an Object DataFrame Mapper (ODM).** This means:

✅ **What we ARE:**

- Type-safe schema definitions with `Col[T]` annotations
- Runtime validation via Pandera
- IDE autocomplete and static type checking
- Multi-backend support (pandas, polars, narwhals)
- Typed column access as class attributes

❌ **What we are NOT:**

- A DataFrame abstraction layer (we don't wrap pandas/polars operations)
- A replacement for native APIs (groupby, merge, join, etc.)
- A DSL for data transformations

**User workflow:**

1. Define schema with Schema → get validation + type safety
2. Access columns via typed attributes → `orders.revenue`
3. Use native DataFrame operations → `orders.fr_data.groupby(...).sum()`

**Feature completeness** means improving the ODM experience (schema composition, validation, introspection), NOT duplicating DataFrame operations.

---

## Completed ✅

### 1. Python 3.9-3.11 Support (+0.4 points)

- ✅ Added `TypeAlias` compatibility via `typing_extensions`
- ✅ Updated `pyproject.toml` to `requires-python = ">=3.9"`
- ✅ Updated classifiers for Python 3.9, 3.10, 3.11
- **Impact**: Expands potential user base by ~70%, removes adoption barrier

## Remaining Tasks to Reach 8/10

### Priority 1: Complete Package Metadata (Quick Win - 2 hours)

**Impact: +0.1 points**

#### Tasks:

- [ ] Update `pyproject.toml` author info (replace placeholder)
- [ ] Set up actual GitHub repository
- [ ] Update all GitHub URLs in:
  - `pyproject.toml` (Homepage, Repository, Bug Tracker)
  - `README.md` (badges, links)
  - `setup.py` if exists
- [ ] Add CONTRIBUTING.md
- [ ] Add CODE_OF_CONDUCT.md

**File to edit:**

```toml
# pyproject.toml
[project]
authors = [
  { name="James [Your Last Name]", email="your.actual@email.com" },
]

[project.urls]
Homepage = "https://github.com/youractual/Schema"
Documentation = "https://Schema.readthedocs.io"  # or GitHub Pages
Repository = "https://github.com/youractual/Schema"
"Bug Tracker" = "https://github.com/youractual/Schema/issues"
```

---

### Priority 2: Performance Benchmarks (Medium - 1 day)

**Impact: +0.2 points**

#### Objective: Prove <5% overhead vs raw DataFrames

#### Tasks:

- [ ] Create `benchmarks/` directory
- [ ] Write benchmark suite comparing:
  - Schema column access vs `df["col"]`
  - Schema validation vs no validation
  - Schema initialization overhead
  - Memory usage comparison
- [ ] Use `pytest-benchmark` or `timeit`
- [ ] Test with datasets of varying sizes (100, 10K, 1M rows)
- [ ] Document results in `PERFORMANCE.md`

**Expected results to document:**

- Column access: <1% overhead (property lookup is fast)
- Validation: One-time cost at construction (~2-3% for typical schemas)
- Memory: Negligible (just wrapper object)

**File structure:**

```
benchmarks/
├── test_column_access.py
├── test_validation_overhead.py
├── test_initialization.py
└── README.md
```

---

### Priority 3: Improve Error Messages (Medium - 1 day)

**Impact: +0.15 points**

#### Current State:

```python
# Generic Pandera error
pandera.errors.SchemaError: Column 'revenue' not in dataframe
```

#### Target State:

```python
# Actionable Schema error
MissingColumnError: Column 'revenue' not found in OrderData schema.

Available columns: ['order_id', 'customer_id', 'item_price', 'quantity_sold']

💡 Tip: Did you mean 'revenu' or 'revenue_total'?
```

#### Tasks:

- [ ] Enhance exception classes with:
  - Contextual information (schema name, available columns)
  - Suggestions using fuzzy matching (`difflib.get_close_matches`)
  - Link to docs for common errors
- [ ] Add colored terminal output (optional, use `rich` or `colorama`)
- [ ] Create error message guide in docs

**Files to edit:**

```python
# src/Schema/backends/pandas_backend.py (and polars_backend.py)
def _translate_pandera_errors(self, error, schema_name, available_cols):
    # Add fuzzy matching
    from difflib import get_close_matches
    suggestions = get_close_matches(missing_col, available_cols, n=3)

    msg = f"Column '{missing_col}' not found in {schema_name} schema.\n\n"
    msg += f"Available columns: {available_cols}\n\n"
    if suggestions:
        msg += f"💡 Tip: Did you mean {suggestions}?"

    raise MissingColumnError(msg)
```

---

### Priority 4: Real-World Case Study (High Value - 2-3 days)

**Impact: +0.25 points**

#### Objective: Demonstrate production-grade usage

#### Option A: ETL Pipeline Example

Create `examples/etl_pipeline/` with:

- [ ] Multi-stage data pipeline (extract → transform → load)
- [ ] 3-4 different Schema schemas
- [ ] Data validation at each stage
- [ ] Error handling and recovery
- [ ] Performance metrics
- [ ] README with learnings

**Example structure:**

```python
# examples/etl_pipeline/schemas.py
class RawOrders(StructFramePandas):
    """Raw e-commerce orders from API."""
    order_id: Col[int]
    customer_email: Col[str] = Field(regex=r'^[\w\.-]+@[\w\.-]+\.\w+$')
    ...

class CleanedOrders(StructFramePandas):
    """Validated and enriched orders."""
    order_id: Col[int] = Field(unique=True)
    customer_id: Col[int]  # Mapped from email
    revenue: Col[float] = Field(ge=0)
    ...

class AggregatedMetrics(StructFramePandas):
    """Daily aggregated business metrics."""
    date: Col[str]
    total_revenue: Col[float]
    total_orders: Col[int]
    avg_order_value: Col[float]
```

#### Option B: ML Workflow Example

Create `examples/ml_workflow/` with:

- [ ] Feature engineering with type-safe schemas
- [ ] Train/validation/test split handling
- [ ] Model input/output validation
- [ ] Integration with sklearn or similar

---

### Priority 5: ODM-Focused Features (Medium - 2 days)

**Impact: +0.15 points**

**Philosophy**: Schema is an **Object DataFrame Mapper**, not a DataFrame abstraction. We provide typed schema access, NOT wrappers around native operations.

❌ **What NOT to build**: `fr_groupby()`, `fr_merge()`, `fr_join()` - these duplicate native APIs and create scope creep

✅ **What TO build**: Features that enhance the ODM experience

#### Option A: Schema Composition & Inheritance

Allow composing schemas from reusable components:

```python
class TimestampMixin(Schema):
    """Reusable timestamp columns."""
    created_at: Col[str]
    updated_at: Col[str]

class UserColumns(Schema):
    """Reusable user identification."""
    user_id: Col[int]
    user_email: Col[str] = Field(regex=r'^[\w\.-]+@')

class Orders(TimestampMixin, UserColumns, StructFramePandas):
    """Composes mixins for DRY schemas."""
    order_id: Col[int] = Field(unique=True)
    revenue: Col[float]
    # Inherits: created_at, updated_at, user_id, user_email
```

**Tasks:**

- [ ] Support multiple inheritance for schema composition
- [ ] Test mixin resolution order
- [ ] Document composition patterns

#### Option B: Computed/Derived Columns

Define columns that are automatically computed:

```python
class Orders(StructFramePandas):
    item_price: Col[float]
    quantity: Col[int]

    @computed_column
    def revenue(self) -> Col[float]:
        """Automatically computed on access."""
        return self.item_price * self.quantity

    @computed_column
    def revenue_normalized(self) -> Col[float]:
        """Can reference other computed columns."""
        return self.revenue / self.revenue.max()
```

**Tasks:**

- [ ] Add `@computed_column` decorator
- [ ] Lazy evaluation on first access
- [ ] Caching strategy
- [ ] Works with validation

#### Option C: Schema Migration Helpers

Help users evolve schemas over time:

```python
class OrdersV1(Schema):
    customer_name: Col[str]  # Old schema
    total: Col[float]

class OrdersV2(Schema):
    customer_id: Col[int]  # New schema (name → id)
    revenue: Col[float]      # renamed from total

# Migration helper
migrated = OrdersV2.fr_migrate_from(
    OrdersV1(old_df),
    mapping={'total': 'revenue'},
    transformers={'customer_name': lambda name: lookup_customer_id(name)}
)
```

**Tasks:**

- [ ] Add `fr_migrate_from()` class method
- [ ] Support column renaming and transformation
- [ ] Validate migration completeness
- [ ] Document migration patterns

**Recommendation**: Start with **Option A (Schema Composition)** - highest value, clearest use case.

---

## Quick Wins (Do These First)

1. **Package metadata** (2 hours) → Immediate credibility boost
2. **Add `PERFORMANCE.md`** with basic benchmarks (4 hours) → Shows you care about overhead
3. **Write one real-world example** in `examples/` (1 day) → Demonstrates practical value

**With these 3 quick wins: 7.9 → 8.2/10** ✅

---

## Beyond 8/10 (Future Roadmap)

### To reach 9/10:

- Community adoption (1000+ GitHub stars)
- Published PyPI package with good download stats
- Integration examples (FastAPI, Dagster, Prefect)
- VS Code extension for schema visualization
- Comprehensive migration guide
- Performance optimizations (zero-overhead column access via `__getattr__`)

### To reach 10/10:

- Battle-tested in major production systems
- Official endorsement from Pandas/Polars teams
- Full DuckDB/cuDF/other backend support
- Framework-level integrations
- Academic paper or conference presentation

---

## Effort vs Impact Matrix

```
                High Impact
                    │
   Package          │  Benchmarks
   Metadata    ─────┼─────  Real-world
                    │       Example
        │           │           │
        │           │           │
─────────────────────────────────── High Effort
        │           │           │
   Error            │  Schema
   Messages    ─────┼─────  Composition
                    │       (ODM features)
                Low Impact
```

**Recommended order:**

1. Package metadata (✅ low effort, medium impact)
2. Python 3.9+ support (✅ DONE)
3. Real-world example (high effort, high impact)
4. Benchmarks (medium effort, high impact)
5. Error messages (medium effort, medium impact)
6. ODM features - schema composition (medium effort, low-medium impact)

**Note on scope**: Schema is an **Object DataFrame Mapper**, not a DataFrame abstraction layer. We focus on typed schemas, validation, and column access - NOT on wrapping DataFrame operations (groupby, joins, etc.). Users should use native pandas/polars APIs via `fr_data` for transformations.

---

## Timeline Estimate

**Fast track (1 week to 8/10):**

- Day 1: Package metadata + basic benchmarks
- Days 2-3: Real-world ETL example
- Day 4: Error message improvements
- Day 5: Polish, docs, testing

**Sustainable pace (2 weeks to 8/10):**

- Week 1: Metadata, benchmarks, start example
- Week 2: Complete example, error messages, docs

---

## Success Metrics

You've reached 8/10 when:

- ✅ Python 3.9+ support (DONE)
- ✅ Real GitHub repo with proper metadata
- ✅ At least one production-quality example in `examples/`
- ✅ Performance overhead documented as <5%
- ✅ Error messages include suggestions and context
- ✅ 2+ external users report successful usage (issues/discussions)

**Current progress: 2/6 complete** (Python 3.9+ support + type safety)
