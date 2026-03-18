from __future__ import annotations

import pandas as pd

from frameright.pandas import Schema
from frameright.typing import Col


class Example(Schema):
    a: Col[int]
    b: Col[float]


df = pd.DataFrame({"a": [1, 2], "b": [1.5, 2.5]})
ex = Example(df, validate=False)


def needs_int(s: pd.Series[int]) -> None:
    _ = s


def needs_float(s: pd.Series[float]) -> None:
    _ = s


needs_int(ex.b)
needs_float(ex.a)
needs_float(ex.a)
needs_float(ex.a)
