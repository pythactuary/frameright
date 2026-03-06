"""Tests for Polars backend integration."""

import pytest
import polars as pl
from datetime import datetime, date
from structframe import StructFrame, Field, FieldInfo
from structframe.typing import Col, Index
from structframe.exceptions import (
    MissingColumnError,
    TypeMismatchError,
    ConstraintViolationError,
)
from typing import Optional


# ---------------------------------------------------------------------------
# Schema Definitions for Testing
# ---------------------------------------------------------------------------


class UserData(StructFrame):
    """Kitchen-sink schema covering all features."""

    user_id: Col[int] = Field(unique=True)
    username: Col[str] = Field(min_length=1)
    is_active: Col[bool]
    engagement_score: Col[float] = Field(ge=0.0, le=100.0)
    tier: Col[str] = Field(alias="SUBSCRIPTION_TIER", isin=["Free", "Pro", "Enterprise"])
    lifetime_value: Optional[Col[float]] = Field(ge=0.0)


class StrictSchema(StructFrame):
    """Schema with strict non-nullable, unique constraints."""

    id: Col[int] = Field(unique=True, nullable=False)
    code: Col[str] = Field(regex=r"^[A-Z]{3}$", nullable=False)
    value: Col[float] = Field(gt=0, lt=1000)


class MinimalSchema(StructFrame):
    """Simplest possible schema."""

    col_a: Col[int]
    col_b: Col[str]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_df():
    """Provides a perfectly valid Polars dataframe."""
    return pl.DataFrame(
        {
            "user_id": [1, 2, 3],
            "username": ["alice", "bob", "charlie"],
            "is_active": [True, False, True],
            "engagement_score": [85.5, 12.0, 99.9],
            "SUBSCRIPTION_TIER": ["Pro", "Free", "Enterprise"],
            "lifetime_value": [150.0, 0.0, 5000.0],
        }
    )


@pytest.fixture
def valid_df_missing_optional(valid_df):
    """Provides a valid dataframe, but drops the optional column."""
    return valid_df.drop("lifetime_value")


@pytest.fixture
def strict_df():
    """Provides a valid dataframe for StrictSchema."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "code": ["ABC", "DEF", "GHI"],
            "value": [10.0, 500.0, 999.0],
        }
    )


# ===========================================================================
# SECTION 1: Initialization & Basic Properties
# ===========================================================================


class TestPolarsInitialization:
    def test_successful_initialization(self, valid_df):
        """Valid Polars data creates a valid object."""
        users = UserData(valid_df)
        assert len(users) == 3
        assert users.sf_backend.name == "polars"

    def test_auto_detects_polars_backend(self, valid_df):
        """Backend is auto-detected as 'polars'."""
        users = UserData(valid_df)
        assert users.sf_backend.name == "polars"

    def test_explicit_backend_parameter(self, valid_df):
        """Explicit backend='polars' works."""
        users = UserData(valid_df, backend="polars")
        assert users.sf_backend.name == "polars"

    def test_copy_flag_creates_independent_copy(self, valid_df):
        """copy=True creates an independent copy."""
        users = UserData(valid_df, copy=True)
        # Polars clones on copy
        assert users.sf_data is not valid_df

    def test_validate_false_skips_validation(self):
        """validate=False skips all checks."""
        df = pl.DataFrame({"wrong": [1, 2, 3]})
        obj = MinimalSchema(df, validate=False)
        assert len(obj) == 3


# ===========================================================================
# SECTION 2: Missing Column Detection
# ===========================================================================


class TestPolarsMissingColumns:
    def test_missing_required_column(self, valid_df):
        """Missing a required column raises MissingColumnError."""
        df = valid_df.drop("username")
        with pytest.raises(MissingColumnError):
            UserData(df)

    def test_missing_optional_column_allowed(self, valid_df_missing_optional):
        """Missing optional column does not raise."""
        users = UserData(valid_df_missing_optional)
        assert users.lifetime_value is None


# ===========================================================================
# SECTION 3: Dtype Validation
# ===========================================================================


class TestPolarsDtypeValidation:
    def test_int_dtype_mismatch(self, valid_df):
        """String values in an int column raises TypeMismatchError."""
        df = valid_df.with_columns(pl.Series("user_id", ["1", "2", "3"]))
        with pytest.raises(TypeMismatchError, match="dtype"):
            UserData(df)

    def test_str_dtype_accepts_utf8(self, valid_df):
        """String columns accept Polars Utf8 type."""
        users = UserData(valid_df)
        assert users.username[0] == "alice"


# ===========================================================================
# SECTION 4: Field-Level Constraint Validation
# ===========================================================================


class TestPolarsFieldConstraints:
    def test_ge_constraint(self, valid_df):
        """ge (>=) constraint rejects values below threshold."""
        df = valid_df.with_columns(pl.Series("engagement_score", [-1.0, 12.0, 99.9]))
        with pytest.raises(ConstraintViolationError, match="greater_than_or_equal_to"):
            UserData(df)

    def test_le_constraint(self, valid_df):
        """le (<=) constraint rejects values above threshold."""
        df = valid_df.with_columns(pl.Series("engagement_score", [150.0, 12.0, 99.9]))
        with pytest.raises(ConstraintViolationError, match="less_than_or_equal_to"):
            UserData(df)

    def test_gt_constraint(self, strict_df):
        """gt (>) constraint rejects values at or below threshold."""
        df = strict_df.with_columns(pl.Series("value", [0.0, 500.0, 999.0]))
        with pytest.raises(ConstraintViolationError, match="greater_than"):
            StrictSchema(df)

    def test_lt_constraint(self, strict_df):
        """lt (<) constraint rejects values at or above threshold."""
        df = strict_df.with_columns(pl.Series("value", [1000.0, 500.0, 999.0]))
        with pytest.raises(ConstraintViolationError, match="less_than"):
            StrictSchema(df)

    def test_isin_constraint(self, valid_df):
        """isin constraint rejects values not in allowed list."""
        df = valid_df.with_columns(
            pl.Series("SUBSCRIPTION_TIER", ["Pro", "SuperPro", "Enterprise"])
        )
        with pytest.raises(ConstraintViolationError, match="isin"):
            UserData(df)

    def test_unique_constraint(self, strict_df):
        """unique=True rejects duplicate values."""
        df = strict_df.with_columns(pl.Series("id", [1, 1, 3]))
        with pytest.raises(ConstraintViolationError, match="field_uniqueness"):
            StrictSchema(df)

    def test_regex_constraint(self, strict_df):
        """regex constraint rejects values not matching pattern."""
        df = strict_df.with_columns(pl.Series("code", ["abc", "DEF", "GHI"]))
        with pytest.raises(ConstraintViolationError, match="str_matches"):
            StrictSchema(df)

    def test_valid_data_passes(self, valid_df):
        """Valid data passes all constraints."""
        users = UserData(valid_df)
        assert len(users) == 3


# ===========================================================================
# SECTION 5: Property Access
# ===========================================================================


class TestPolarsPropertyAccess:
    def test_getter_returns_series(self, valid_df):
        """Property getter returns a Polars Series."""
        users = UserData(valid_df)
        assert isinstance(users.user_id, pl.Series)

    def test_getter_returns_correct_values(self, valid_df):
        """Property getter returns correct data."""
        users = UserData(valid_df)
        assert users.user_id.to_list() == [1, 2, 3]

    def test_alias_mapping(self, valid_df):
        """Aliased column maps correctly."""
        users = UserData(valid_df)
        assert users.tier.to_list() == ["Pro", "Free", "Enterprise"]

    def test_setter(self, valid_df):
        """Property setter updates the underlying DataFrame."""
        users = UserData(valid_df, validate=False)
        users.user_id = pl.Series("user_id", [10, 20, 30])
        assert users.user_id.to_list() == [10, 20, 30]


# ===========================================================================
# SECTION 6: Core Methods
# ===========================================================================


class TestPolarsCoreMethods:
    def test_sf_data_returns_polars_df(self, valid_df):
        """sf_data returns the underlying Polars DataFrame."""
        users = UserData(valid_df)
        assert isinstance(users.sf_data, pl.DataFrame)

    def test_sf_filter(self, valid_df):
        """sf_filter returns a new StructFrame with filtered rows."""
        users = UserData(valid_df)
        active = users.sf_filter(users.is_active)
        assert len(active) == 2
        assert isinstance(active, UserData)

    def test_sf_to_dict(self, valid_df):
        """sf_to_dict returns a list of dicts."""
        users = UserData(valid_df)
        d = users.sf_to_dict(orient="records")
        assert isinstance(d, list)
        assert len(d) == 3

    def test_sf_to_csv(self, valid_df, tmp_path):
        """sf_to_csv writes a CSV file."""
        users = UserData(valid_df)
        path = str(tmp_path / "test.csv")
        users.sf_to_csv(path)
        loaded = pl.read_csv(path)
        assert loaded.height == 3


# ===========================================================================
# SECTION 7: Factory Methods
# ===========================================================================


class TestPolarsFactoryMethods:
    def test_sf_from_dict(self):
        """sf_from_dict creates from dict with Polars backend."""
        data = {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"]}
        obj = MinimalSchema.sf_from_dict(data, backend="polars")
        assert isinstance(obj.sf_data, pl.DataFrame)
        assert len(obj) == 3

    def test_sf_from_records(self):
        """sf_from_records creates from list of dicts."""
        records = [
            {"col_a": 1, "col_b": "a"},
            {"col_a": 2, "col_b": "b"},
        ]
        obj = MinimalSchema.sf_from_records(records, backend="polars")
        assert isinstance(obj.sf_data, pl.DataFrame)
        assert len(obj) == 2

    def test_sf_from_csv(self, tmp_path):
        """sf_from_csv loads from CSV using Polars."""
        csv_path = str(tmp_path / "test.csv")
        pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]}).write_csv(csv_path)
        obj = MinimalSchema.sf_from_csv(csv_path, backend="polars")
        assert isinstance(obj.sf_data, pl.DataFrame)
        assert len(obj) == 2

    def test_sf_example(self):
        """sf_example generates dummy Polars data."""
        obj = MinimalSchema.sf_example(nrows=5, backend="polars")
        assert isinstance(obj.sf_data, pl.DataFrame)
        assert len(obj) == 5


# ===========================================================================
# SECTION 8: Coercion
# ===========================================================================


class TestPolarsCoercion:
    def test_coerce_int_column(self):
        """Coerce string→int on Polars."""
        df = pl.DataFrame({"col_a": ["1", "2", "3"], "col_b": ["a", "b", "c"]})
        obj = MinimalSchema.sf_coerce(df, backend="polars")
        assert obj.sf_data["col_a"].dtype == pl.Int64

    def test_coerce_bool_from_string(self):
        """Coerce string→bool on Polars."""

        class BoolSchema(StructFrame):
            flag: Col[bool]

        df = pl.DataFrame({"flag": ["true", "false", "yes"]})
        obj = BoolSchema.sf_coerce(df, backend="polars")
        assert obj.flag.to_list() == [True, False, True]


# ===========================================================================
# SECTION 9: Python Protocols
# ===========================================================================


class TestPolarsProtocols:
    def test_len(self, valid_df):
        assert len(UserData(valid_df)) == 3

    def test_repr(self, valid_df):
        r = repr(UserData(valid_df))
        assert "UserData" in r
        assert "polars" in r
        assert "3 rows" in r

    def test_iter(self, valid_df):
        users = UserData(valid_df)
        rows = list(users)
        assert len(rows) == 3

    def test_eq(self, valid_df):
        a = UserData(valid_df, copy=True)
        b = UserData(valid_df, copy=True)
        assert a == b

    def test_contains(self, valid_df):
        users = UserData(valid_df)
        assert "user_id" in users
        assert "nonexistent" not in users


# ===========================================================================
# SECTION 10: Schema Introspection
# ===========================================================================


class TestPolarsSchemaIntrospection:
    def test_schema_info(self):
        """sf_schema_info can return a Polars DataFrame."""
        info = MinimalSchema.sf_schema_info(backend="polars")
        assert isinstance(info, pl.DataFrame)
        assert info.height == 2


# ===========================================================================
# SECTION 11: Backend Registry
# ===========================================================================


class TestBackendRegistry:
    def test_detect_pandas(self):
        """detect_backend returns pandas adapter for pd.DataFrame."""
        import pandas as pd
        from structframe import detect_backend

        adapter = detect_backend(pd.DataFrame())
        assert adapter.name == "pandas"

    def test_detect_polars(self):
        """detect_backend returns polars adapter for pl.DataFrame."""
        from structframe import detect_backend

        adapter = detect_backend(pl.DataFrame())
        assert adapter.name == "polars"

    def test_detect_unknown_type(self):
        """detect_backend raises TypeError for unknown types."""
        from structframe import detect_backend

        with pytest.raises(TypeError, match="No StructFrame backend"):
            detect_backend({"a": [1, 2, 3]})

    def test_get_backend_by_name(self):
        """get_backend returns the correct adapter."""
        from structframe import get_backend

        assert get_backend("pandas").name == "pandas"
        assert get_backend("polars").name == "polars"

    def test_get_backend_unknown(self):
        """get_backend raises ValueError for unknown backends."""
        from structframe import get_backend

        with pytest.raises(ValueError, match="Unknown backend"):
            get_backend("cudf")

    def test_register_custom_backend(self):
        """register_backend allows custom backends."""
        from structframe import register_backend

        register_backend("my_custom", "some.module.path")
        # Registration successful — loading would fail since module doesn't exist
        # but that's fine for this test


# ===========================================================================
# SECTION 12: Cross-backend consistency
# ===========================================================================


class TestCrossBackendConsistency:
    def test_same_schema_both_backends(self):
        """Same schema works with both Pandas and Polars data."""
        import pandas as pd

        pd_df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        pl_df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

        pd_obj = MinimalSchema(pd_df)
        pl_obj = MinimalSchema(pl_df)

        assert len(pd_obj) == len(pl_obj) == 2
        assert pd_obj.sf_backend.name == "pandas"
        assert pl_obj.sf_backend.name == "polars"

    def test_to_dict_consistent(self):
        """to_dict returns same structure for both backends."""
        import pandas as pd

        pd_df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        pl_df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

        pd_obj = MinimalSchema(pd_df)
        pl_obj = MinimalSchema(pl_df)

        pd_dict = pd_obj.sf_to_dict(orient="records")
        pl_dict = pl_obj.sf_to_dict(orient="records")

        assert pd_dict == pl_dict
