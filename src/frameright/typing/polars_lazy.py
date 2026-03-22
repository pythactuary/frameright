"""Polars lazy mode (LazyFrame) types for Schema.

Use these imports when your Schema subclass wraps a **Polars LazyFrame**:

    from frameright.typing.polars_lazy import Col

At type-check time ``Col[T]`` resolves to ``pl.Expr`` so that:

* Your IDE autocompletes Polars expression methods (``filter``, ``over``, ``alias``, etc.)
* Your schema annotations still use ``Col[T]`` (e.g. ``score: Col[float]``) for documentation
    and editor tooling. Polars expressions are not currently typed as ``pl.Expr[T]`` upstream,
    so type checkers generally treat the runtime value as ``pl.Expr`` (inner type is best-effort).

At runtime ``Col`` is identical to the generic sentinel from
``Schema.typing`` (used by ``__init_subclass__`` for schema parsing).

**Important**: When using LazyFrame:
- Column access returns ``pl.Expr`` (lazy expressions)
- You must call ``.collect()`` to materialize results
- Aggregations like ``.sum()`` return expressions, not concrete values

For **DataFrame** (eager) support, use ``polars_eager`` instead.
"""

from typing import TYPE_CHECKING, Generic, TypeVar

from frameright.typing import Col as _RuntimeCol

T = TypeVar("T")

if TYPE_CHECKING:
    import polars as pl

    class ColTemp(Generic[T], pl.Expr):
        """Polars Expr type for Schema lazy schemas."""

        ...

    Col = (
        ColTemp[T] | pl.Expr
    )  # polars expressions can be untyped, so we allow Col[T] or plain pl.Expr

else:
    Col = _RuntimeCol


__all__ = ["Col"]
