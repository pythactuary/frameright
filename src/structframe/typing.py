import pandas as pd
from typing import TypeVar, Generic, TYPE_CHECKING, TypeAlias

T = TypeVar("T")

if TYPE_CHECKING:
    Col: TypeAlias = pd.Series[T]
    Index: TypeAlias = pd.Index[T]
else:

    class Col(Generic[T]):
        pass

    class Index(Generic[T]):
        pass
