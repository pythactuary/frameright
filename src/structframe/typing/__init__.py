"""Generic column and index types for StructFrame.

At type-check time (mypy, Pylance, PyCharm), ``Col[T]`` resolves to
``pd.Series[T]`` so that IDE autocomplete and static analysis work.
At runtime ``Col`` is a lightweight generic sentinel used by
``__init_subclass__`` to detect annotated columns.
"""

import sys
import pandas as pd
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

T = TypeVar("T")

if TYPE_CHECKING:
    # Static type checkers see Col as pd.Series and Index as pd.Index.
    # This gives full autocomplete and type safety in IDEs.
    # When Polars DataFrames are used, the runtime adapter handles
    # the actual column types; the static hints remain pd.Series
    # because there is no Union-based way to overload at check time.
    Col: TypeAlias = pd.Series[Any]
    Index: TypeAlias = pd.Index[Any]
else:

    class Col(Generic[T]):
        """Runtime sentinel for column type annotations."""

        pass

    class Index(Generic[T]):
        """Runtime sentinel for index type annotations."""

        pass


__all__ = ["Col", "Index"]
