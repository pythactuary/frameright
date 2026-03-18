"""Narwhals-specific type aliases for Schema.

Use these when working with narwhals DataFrames for backend-agnostic code::

    from frameright import Schema, Field
    from frameright.typing.narwhals import Col
    import narwhals as nw

    class MyFrame(Schema):
        col_a: Col[int]  # Type checkers see nw.Series
        col_b: Col[str]

    df = nw.from_native(some_df)
    frame = MyFrame(df)
    frame.col_a  # Returns nw.Series, IDE shows narwhals methods
"""

from typing import TYPE_CHECKING, Generic, TypeVar

from frameright.typing import Col as _RuntimeCol

T = TypeVar("T")

if TYPE_CHECKING:
    import narwhals as nw

    # Type checkers see narwhals Series; narwhals is not fully generic upstream,
    # so the inner type T is best-effort today.
    class ColTemp(nw.Series, Generic[T]):
        """Narwhals column with schema-level inner type *T* (best-effort in type checkers)."""

        ...

    Col = ColTemp[T] | nw.Series

    # Narwhals doesn't have Index, use pandas as fallback for type checking

else:
    # At runtime, use the same sentinel as the default typing module
    Col = _RuntimeCol


__all__ = ["Col"]
