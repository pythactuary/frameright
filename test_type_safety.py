"""Demonstrate type safety improvements.

This file shows that type checkers will now catch DataFrame type mismatches.
Run with: pyright test_type_safety.py
"""

import pandas as pd
import polars as pl

from frameright.pandas import Col as PdCol
from frameright.pandas import Schema as PandasSchema
from frameright.polars.eager import Col as PlCol
from frameright.polars.eager import Schema as PolarsSchema


# Correct usage - pandas DataFrame with pandas Schema
class SalesPandas(PandasSchema):
    revenue: PdCol[float]


pd_df = pd.DataFrame({"revenue": [100.0, 200.0]})
sales_pd = SalesPandas(pd_df)  # ✓ Type checker: OK
print(f"✓ Pandas schema with pandas DataFrame: {type(sales_pd.fr_data).__name__}")


# Correct usage - polars DataFrame with polars Schema
class SalesPolars(PolarsSchema):
    revenue: PlCol[float]


pl_df = pl.DataFrame({"revenue": [100.0, 200.0]})
sales_pl = SalesPolars(pl_df)  # ✓ Type checker: OK
print(f"✓ Polars schema with polars DataFrame: {type(sales_pl.fr_data).__name__}")

# WRONG: pandas DataFrame with polars Schema
# Type checker should report an error here:
# "Argument of type 'DataFrame' cannot be assigned to parameter 'df' of type 'pl.DataFrame'"
sales_wrong1 = SalesPolars(pd_df)  # ✗ Type checker: ERROR (expected)
print(f"✗ Type mismatch (should have been caught): {type(sales_wrong1.fr_data).__name__}")

# WRONG: polars DataFrame with pandas Schema
# Type checker should report an error here:
# "Argument of type 'DataFrame' cannot be assigned to parameter 'df' of type 'pd.DataFrame'"
sales_wrong2 = SalesPandas(pl_df)  # ✗ Type checker: ERROR (expected)
print(f"✗ Type mismatch (should have been caught): {type(sales_wrong2.fr_data).__name__}")

print(
    "\nNote: The runtime errors above demonstrate that type checking would have prevented these bugs!"
)
# "Argument of type 'DataFrame' cannot be assigned to parameter 'df' of type 'pd.DataFrame'"
sales_wrong2 = SalesPandas(pl_df)  # ✗ Type checker: ERROR (expected)
print(f"✗ Type mismatch (should have been caught): {type(sales_wrong2.fr_data).__name__}")

print(
    "\nNote: The runtime errors above demonstrate that type checking would have prevented these bugs!"
)
