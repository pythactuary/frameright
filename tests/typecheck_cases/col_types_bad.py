from __future__ import annotations

import pandas as pd

from proteusframe import ProteusFrame
from proteusframe.typing import Col


class Example(ProteusFrame):
    a: Col[int]
    b: Col[float]


df = pd.DataFrame({"a": [1, 2], "b": [1.5, 2.5]})
ex = Example(df, validate=False)

# These should be rejected by a static type checker.
bad_int: pd.Series[int] = ex.b
bad_float: pd.Series[float] = ex.a
