"""
Polars column types for ProteusFrame (compatibility shim).

**Recommended:**
    - For eager DataFrame: from proteusframe.typing.polars_eager import Col
    - For lazy LazyFrame: from proteusframe.typing.polars_lazy import Col

This file exists for backward compatibility and re-exports Col as eager mode.
All generic typing logic is in polars_eager and polars_lazy.
"""

from proteusframe.typing.polars_eager import Col

__all__ = ["Col"]
