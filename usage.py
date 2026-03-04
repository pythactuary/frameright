from __future__ import annotations
import pandas as pd
from structframe import StructFrame


class TestSchema(StructFrame):
    col_a: pd.Series[int]
    """This is a docstring for col_a"""
    col_b: pd.Series[str]
    """This is a docstring for col_b"""


df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["a", "b", "c"]})
obj = TestSchema(df)


class RiskProfile(StructFrame):
    limit: pd.Series[float]
    """The limit of the policy"""
    attachment: pd.Series[float]
    """The attachment point of the policy"""
    premium: pd.Series[float]
    """The premium of the policy"""
    currency: pd.Series[str]
    """The currency of the policy"""


df = pd.DataFrame(
    {
        "limit": [1000000.0, 2000000.0],
        "attachment": [500000.0, 1500000.0],
        "premium": [10000.0, 20000.0],
        "currency": ["USD", "USD"],
    }
)
risk_profile = RiskProfile(df)

underlying_attachment_plus_limit = risk_profile.attachment + risk_profile.limit

print(RiskProfile.get_schema())
