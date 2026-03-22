"""Generic column type for Schema.

Col[T] is a generic type used for type annotations in Schema schemas.

For backend-specific imports:
- ``from frameright.typing.pandas import Col``
- ``from frameright.typing.polars_eager import Col``  # Polars DataFrame (eager)
- ``from frameright.typing.polars_lazy import Col``   # Polars LazyFrame (lazy)
- ``from frameright.typing.narwhals import Col``

At runtime, Col is a lightweight generic sentinel used by
``__init_subclass__`` to detect annotated columns. At type-check time,
it's a generic type that allows IDE autocompletion and type hints.

Typing note: with ``pandas-stubs`` installed, type checkers can treat
``Col[T]`` as ``pd.Series[T]``. Other backends are best-effort because
their upstream libraries do not currently expose fully generic ``Series[T]``
/ ``Expr[T]`` types.
"""

import sys
from datetime import date, datetime
from typing import TYPE_CHECKING, Generic, TypeVar

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

import pandas as pd

T = TypeVar(
    "T", int, float, str, pd.DatetimeTZDtype, datetime, date
)  # Type variable for column data types

if TYPE_CHECKING:
    # Type checkers see pandas Series, preserving inner type T

    Col: TypeAlias = pd.Series[T]


else:

    class Col(Generic[T]):
        """Generic column type marker for Schema schemas."""

        pass


__all__ = ["Col"]
