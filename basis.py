class BaseClass:
    """Base class for all ProteusFrame classes."""

    pass


class MySchema(BaseClass):
    """Example schema class."""

    a: int
    "A variable"


d = MySchema()

d.a
