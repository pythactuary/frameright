"""Pandas-specific column types for Schema.

Use these imports when your Schema subclass wraps a **pandas** DataFrame:

    from frameright.typing.pandas import Col

At type-check time ``Col[T]`` resolves to ``pd.Series[T]``,
giving full IDE autocomplete and static analysis.
At runtime ``Col`` is identical to the generic sentinel from
``frameright.typing`` (used by ``__init_subclass__`` for schema parsing).
"""

from frameright.typing import Col as _RuntimeCol

# Re-export the generic sentinel
Col = _RuntimeCol  # type: ignore[type-arg]


__all__ = ["Col"]
