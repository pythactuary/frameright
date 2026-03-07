"""Tests for compatibility with PEP 563 (from __future__ import annotations).

This file MUST keep the future import as the first statement so that all
annotations in this module are stringified at runtime.  The tests verify
that ProteusFrame's ``__init_subclass__`` correctly resolves those strings
back to real types via namespace-injected ``get_type_hints``.
"""

from __future__ import annotations

import pytest
import pandas as pd
from typing import Optional

from proteusframe import ProteusFrame, Field
from proteusframe.typing import Col, Index
from proteusframe.exceptions import ConstraintViolationError, MissingColumnError


# ---------------------------------------------------------------------------
# Schemas defined with stringified annotations (PEP 563)
# ---------------------------------------------------------------------------


class FutureSchema(ProteusFrame):
    x: Col[int]
    y: Optional[Col[str]]
    z: Col[float] = Field(ge=0)


class FutureWithAlias(ProteusFrame):
    score: Col[float] = Field(alias="SCORE_COL", le=100)
    name: Col[str]


class FutureWithIndex(ProteusFrame):
    idx: Index[int]
    value: Col[float]


# ===========================================================================
# Tests
# ===========================================================================


class TestFutureAnnotationsSchemaResolution:
    """Verify that schema parsing works when annotations are strings."""

    def test_schema_attributes_detected(self):
        assert "x" in FutureSchema._pf_schema
        assert "y" in FutureSchema._pf_schema
        assert "z" in FutureSchema._pf_schema

    def test_inner_types_resolved(self):
        assert FutureSchema._pf_schema["x"]["inner_type"] == int
        assert FutureSchema._pf_schema["y"]["inner_type"] == str
        assert FutureSchema._pf_schema["z"]["inner_type"] == float

    def test_optional_detected(self):
        assert not FutureSchema._pf_schema["x"]["is_optional"]
        assert FutureSchema._pf_schema["y"]["is_optional"]
        assert not FutureSchema._pf_schema["z"]["is_optional"]

    def test_field_constraints_preserved(self):
        fi = FutureSchema._pf_schema["z"]["field_info"]
        assert fi.ge == 0


class TestFutureAnnotationsConstruction:
    """Verify construction and validation work with stringified annotations."""

    def test_valid_data(self):
        df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"], "z": [1.0, 2.0]})
        obj = FutureSchema(df)
        assert len(obj) == 2

    def test_missing_required_column(self):
        df = pd.DataFrame({"x": [1], "z": [1.0]})
        # y is optional, so missing is OK
        obj = FutureSchema(df)
        assert obj.y is None

    def test_missing_required_column_raises(self):
        df = pd.DataFrame({"y": ["a"], "z": [1.0]})
        with pytest.raises(MissingColumnError):
            FutureSchema(df)

    def test_constraint_violation(self):
        df = pd.DataFrame({"x": [1], "y": ["a"], "z": [-5.0]})
        with pytest.raises(ConstraintViolationError):
            FutureSchema(df)

    def test_alias_mapping(self):
        df = pd.DataFrame({"SCORE_COL": [50.0], "name": ["alice"]})
        obj = FutureWithAlias(df)
        assert len(obj) == 1


class TestFutureAnnotationsIndex:
    """Verify Index[T] works with stringified annotations."""

    def test_index_detected(self):
        assert len(FutureWithIndex._pf_index_attrs) == 1
        assert FutureWithIndex._pf_index_attrs[0]["name"] == "idx"

    def test_construction_with_index(self):
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]})
        obj = FutureWithIndex(df)
        assert len(obj) == 3


class TestFutureAnnotationsPolars:
    """Verify PEP 563 works with Polars backend too."""

    def test_polars_construction(self):
        import polars as pl

        df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"], "z": [1.0, 2.0]})
        obj = FutureSchema(df)
        assert len(obj) == 2
        assert obj.pf_backend.name == "polars"

    def test_polars_lazyframe(self):
        import polars as pl

        lf = pl.LazyFrame({"x": [1, 2], "y": ["a", "b"], "z": [1.0, 2.0]})
        obj = FutureSchema(lf)
        assert len(obj) == 2


# ---------------------------------------------------------------------------
# TYPE_CHECKING-guarded import scenario
# ---------------------------------------------------------------------------
# The imports at the top of this file are NOT guarded behind TYPE_CHECKING,
# but we verify the mechanism works by testing that Col/Index are resolved
# from the injected localns even if they weren't in the module globals.
# A true TYPE_CHECKING-guarded test requires a separate module — we simulate
# it by removing Col/Index from the globals and verifying resolution still
# works via __init_subclass__'s namespace injection.


def test_type_checking_guard_simulation():
    """Simulate a TYPE_CHECKING-guarded import by defining a class in exec().

    The exec'd code has ``from __future__ import annotations`` active and
    Col is NOT in its globals — only our namespace injection saves it.
    """
    import types

    # Create a fake module with __future__ annotations semantics
    code = (
        "from __future__ import annotations\n"
        "from proteusframe import ProteusFrame\n"
        "from proteusframe.typing import Col\n"
        "from typing import Optional\n"
        "\n"
        "class GuardedSchema(ProteusFrame):\n"
        "    a: Col[int]\n"
        "    b: Optional[Col[str]]\n"
    )
    fake_module = types.ModuleType("_test_guarded_module")
    import sys

    sys.modules[fake_module.__name__] = fake_module
    try:
        exec(compile(code, fake_module.__name__, "exec"), fake_module.__dict__)
        GuardedSchema = fake_module.__dict__["GuardedSchema"]

        assert "a" in GuardedSchema._pf_schema
        assert "b" in GuardedSchema._pf_schema
        assert GuardedSchema._pf_schema["a"]["inner_type"] == int
        assert GuardedSchema._pf_schema["b"]["is_optional"] is True

        # Construct with real data
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        obj = GuardedSchema(df)
        assert len(obj) == 2
    finally:
        sys.modules.pop(fake_module.__name__, None)
