from __future__ import annotations

import pandas as pd

from proteusframe import ProteusFrame
from proteusframe.typing import Col


class Example(ProteusFrame):
    a: Col[int]
    b: Col[float]


def needs_int(s: pd.Series[int]) -> None:
    _ = s


def needs_float(s: pd.Series[float]) -> None:
    _ = s


df = pd.DataFrame({"a": [1, 2], "b": [1.5, 2.5]})
ex = Example(df, validate=False)

# With pandas stubs installed, attribute accessors type-check as pd.Series[T].
needs_int(ex.a)
needs_float(ex.b)
