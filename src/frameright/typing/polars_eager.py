"""Polars eager mode (DataFrame) types for Schema.

Use these imports when your Schema subclass wraps a **Polars DataFrame** (eager):

    from frameright.typing.polars_eager import Col

At type-check time ``Col[T]`` resolves to ``pl.Series`` so that:

* Your IDE autocompletes Polars Series methods (``sum``, ``mean``, ``to_list``, etc.)
* Your schema annotations still use ``Col[T]`` (e.g. ``score: Col[float]``) for documentation
    and editor tooling. Note that Polars itself is not currently typed as ``pl.Series[T]`` upstream,
    so most type checkers will treat the runtime value as unparameterized ``pl.Series``.

At runtime ``Col`` is identical to the generic sentinel from
``Schema.typing`` (used by ``__init_subclass__`` for schema parsing).

For **LazyFrame** support, use ``polars_lazy`` instead.
"""

from typing import TYPE_CHECKING, Generic, TypeVar

from frameright.typing import Col as _RuntimeCol

T = TypeVar("T")


if TYPE_CHECKING:
    import polars as pl

    class ColTemp(Generic[T], pl.Series):
        """Polars Series type for Schema eager schemas."""

        ...

    Col = (
        ColTemp[T] | pl.Series
    )  # polars expressions can be untyped, so we allow Col[T] or plain pl.Expr

else:
    Col = _RuntimeCol


__all__ = ["Col"]
