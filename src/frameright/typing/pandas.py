"""Pandas-specific column and index types for Schema.

Use these imports when your Schema subclass wraps a **pandas** DataFrame:

    from frameright.typing.pandas import Col, Index

At type-check time ``Col[T]`` resolves to ``pd.Series[T]``,
giving full IDE autocomplete and static analysis.
At runtime ``Col`` is identical to the generic sentinel from
``frameright.typing`` (used by ``__init_subclass__`` for schema parsing).
"""

from frameright.typing import Col as _RuntimeCol
from frameright.typing import Index as _RuntimeIndex

# Re-export the generic sentinels
# We don't alias to pd.Series/pd.Index to avoid double-specialization errors
Col = _RuntimeCol
Index = _RuntimeIndex


__all__ = ["Col", "Index"]
