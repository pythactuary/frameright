"""Pandas-specific column and index types for StructFrame.

Use these imports when your StructFrame subclass wraps a **pandas** DataFrame:

    from structframe.typing.pandas import Col, Index

At type-check time ``Col[T]`` resolves to ``pd.Series[T]``,
giving full IDE autocomplete and static analysis.
At runtime ``Col`` is identical to the generic sentinel from
``structframe.typing`` (used by ``__init_subclass__`` for schema parsing).
"""

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

import pandas as pd

from structframe.typing import Col as _RuntimeCol, Index as _RuntimeIndex

if TYPE_CHECKING:
    Col: TypeAlias = pd.Series[Any]
    Index: TypeAlias = pd.Index[Any]
else:
    Col = _RuntimeCol
    Index = _RuntimeIndex


__all__ = ["Col", "Index"]
