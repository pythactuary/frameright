"""Polars-specific column and index types for ProteusFrame.

Use these imports when your ProteusFrame subclass wraps a **Polars** DataFrame:

    from proteusframe.typing.polars import Col, Index

At type-check time ``Col[T]`` resolves to a generic subclass of ``pl.Expr``
so that:

* Your IDE autocompletes Polars expression methods (``filter``, ``sum``,
  ``over``, etc.) rather than pandas ``Series`` methods.
* The inner type ``T`` is preserved — hovering over ``score: Col[float]``
  shows ``Col[float]``, not just ``pl.Expr``.

At runtime ``Col`` is identical to the generic sentinel from
``proteusframe.typing`` (used by ``__init_subclass__`` for schema parsing).
"""

from typing import TYPE_CHECKING, Generic, TypeVar

from proteusframe.typing import Col as _RuntimeCol, Index as _RuntimeIndex

T = TypeVar("T")

if TYPE_CHECKING:
    import polars as pl

    class Col(pl.Expr, Generic[T]):
        """Polars column expression that preserves the inner type *T*."""

        ...

    class Index(pl.Expr, Generic[T]):
        """Polars index expression that preserves the inner type *T*."""

        ...

else:
    Col = _RuntimeCol
    Index = _RuntimeIndex


__all__ = ["Col", "Index"]
