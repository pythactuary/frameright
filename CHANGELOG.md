# Changelog

All notable changes to ProteusFrame will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Python 3.9+ support**: Extended compatibility from Python 3.12+ to Python 3.9+ by conditionally importing `TypeAlias` from `typing_extensions`

### Changed

#### Backend Selection Simplification

- **Backend-specific classes (recommended)**: Added `ProteusFramePandas`, `ProteusFramePolars`, `ProteusFramePolarsLazy`, `ProteusFrameNarwhals`, and `ProteusFrameNarwhalsLazy` as explicit backend classes for better type safety
- **Pandas default**: Base `ProteusFrame` class now defaults to pandas backend for backward compatibility (previously required explicit backend parameter or auto-detection)
- **Explicit over implicit**: Users can choose between:
  - Backend-specific classes (e.g., `class Sales(ProteusFramePandas)`) for strongest type guarantees
  - Base `ProteusFrame` with optional `backend` parameter (e.g., `Sales(df, backend="polars")`)
  - Base `ProteusFrame` with no parameter (defaults to pandas)
- **Type system improvements**: Removed `Generic[T]` from base class, simplified type inference, fewer type checkers issues

## [0.3.0] - 2026-03-06

### Added

#### Multi-Backend Architecture

- **Backend adapter pattern**: Abstract `BackendAdapter` ABC with pluggable implementations for Pandas, Polars, and future backends (cuDF, etc.)
- **Auto-detection**: Automatically detects DataFrame type and loads the appropriate backend
- **Registry system**: `register_backend()` allows third-party backends to be registered without modifying ProteusFrame source
- **Backend selection**: Explicit `backend=` parameter in constructors and factory methods

#### Polars Support

- **Full Polars backend**: Complete implementation supporting both eager `pl.DataFrame` and lazy `pl.LazyFrame`
- **Lazy expression properties**: Property getters return `pl.col()` expressions instead of materialized Series, preserving the Polars query optimizer
- **Expression-aware setters**: `set_column()` handles `pl.Expr`, `pl.Series`, and scalar values via `with_columns()`
- **LazyFrame transparency**: All ProteusFrame operations (`filter`, `to_dict`, `to_csv`, `validate`) work seamlessly on LazyFrames
- **LazyFrame materialisation**: Escape hatch to materialize LazyFrames when needed

#### Backend-Specific Typing

- **`proteusframe.typing.pandas`**: Pandas-specific `Col[T]` → `pd.Series[T]` for perfect IDE autocomplete
- **`proteusframe.typing.polars`**: Polars-specific `Col[T]` as `class Col(pl.Expr, Generic[T])` — preserves inner type `T` while giving Polars expression method autocomplete
- **Generic fallback**: Default `proteusframe.typing` works for any backend

#### Validation with Pandera

- **Pandera integration**: Replaced custom validation with Pandera's battle-tested validators
- **Backend-aware schemas**: Uses `pandera.pandas` for Pandas, `pandera.polars` for Polars
- **Error translation**: Pandera exceptions automatically translated to ProteusFrame exception types (`MissingColumnError`, `TypeMismatchError`, `ConstraintViolationError`)

#### Python 3.9+ Compatibility

- **`from __future__ import annotations` support**: Namespace injection in `get_type_hints()` resolves stringified annotations correctly
- **TYPE_CHECKING guards**: Works with `if TYPE_CHECKING:` import patterns
- **typing-extensions fallback**: Uses `typing_extensions.TypeAlias` for Python 3.9

### Fixed

- **Nullable numeric constraints**: `dropna()` before applying numeric checks (ge, gt, le, lt) to avoid NaN comparison issues
- **Integer coercion**: Always attempt `Int64` dtype for nullable integers
- **Boolean coercion**: String → bool mapping now uses `astype(object)` first for compatibility with string dtypes
- **LazyFrame width warning**: Use `collect_schema().len()` instead of `.width` to avoid performance warnings

### Changed

- **Property getters**: Now use `get_column_ref()` instead of `get_column()` — returns lazy expressions for Polars, materialized Series for Pandas
- **Polars immutability**: All DataFrame operations return new instances (via `with_columns()`) respecting Polars' immutable design
- **Module structure**: `typing.py` converted to `typing/` package with backend-specific submodules

### Testing

- **203 tests total**: Up from 118 in v0.2.0
- **Cross-backend test suite**: Full coverage of both Pandas and Polars backends
- **LazyFrame tests**: 8 new tests for LazyFrame construction, filtering, setters, and materialisation
- **Expression chaining tests**: 5 new tests for Polars expression composition (arithmetic, comparisons, string methods)
- **Future annotations tests**: 15 new tests in `test_future_annotations.py` covering PEP 563 compatibility

## [0.2.0] - 2024-XX-XX

### Added

- Initial multi-backend exploration
- Basic Pandera integration

### Fixed

- Bug fixes for nullable constraints, integer coercion, boolean coercion

## [0.1.0] - 2024-XX-XX

### Added

- Initial release
- Pandas-only support
- Type-safe DataFrame wrappers
- Pydantic-inspired `Field()` API
- Runtime validation
- IDE autocomplete via `Col[T]` type hints

[0.3.0]: https://github.com/yourusername/proteusframe/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yourusername/proteusframe/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yourusername/proteusframe/releases/tag/v0.1.0
