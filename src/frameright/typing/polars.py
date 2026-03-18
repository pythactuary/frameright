"""
Polars column types for Schema (compatibility shim).

**Recommended:**
    - For eager DataFrame: from frameright.typing.polars_eager import Col
    - For lazy LazyFrame: from frameright.typing.polars_lazy import Col

This file exists for backward compatibility and re-exports Col as eager mode.
All generic typing logic is in polars_eager and polars_lazy.
"""

from frameright.typing.polars_eager import Col

__all__ = ["Col"]
