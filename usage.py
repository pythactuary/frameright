from __future__ import annotations
import pandas as pd
from structframe import StructFrame, Field
from structframe.typing import Col, Index
from typing import Optional


class TestSchema(StructFrame):
    col_a: Col[int]
    """This is a docstring for col_a"""
    col_b: Col[str]
    """This is a docstring for col_b"""


df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["a", "b", "c"]})
obj = TestSchema(df)

obj.col_a


class RiskProfile(StructFrame):
    """A class representing the risk profile of an insurance policy"""

    limit: Col[float] = Field(ge=0)
    """The limit of the policy"""
    attachment: Col[float] = Field(ge=0)
    """The attachment point of the policy"""
    premium: Col[float]
    """The premium of the policy"""
    currency: Col[str]
    """The currency of the policy"""
    country: Optional[Col[str]]
    """The country of the policy (optional)"""


df = pd.DataFrame(
    {
        "limit": [1000000.0, 0],
        "attachment": [500000.0, 1500000.0],
        "premium": [10000.0, 20000.0],
        "currency": ["USD", 1],
    }
)
risk_profile = RiskProfile(df)

underlying_attachment_plus_limit = risk_profile.attachment + risk_profile.limit
country = risk_profile.country
currency = risk_profile.currency


class Orders(StructFrame):
    item_ref: Index[str]
    """Reference ID for the item"""
    item_price: Col[float]
    """The price per unit of the item"""
    quantity_sold: Col[int]
    """Number of units sold"""
    revenue: Optional[Col[float]]


df = pd.DataFrame({"item_price": [10.0, 20.0, 15.0], "quantity_sold": [100, 150, 200]})
orders = Orders(df)
orders.revenue = orders.item_price * orders.quantity_sold

print(orders.sf_data)
# Perfect IDE autocomplete, completely type-safe, beautiful syntax
total_revenue = orders.revenue.sum()
print(f"Total Revenue: {total_revenue}")
