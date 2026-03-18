"""Tests for Polars backend integration."""

from typing import Optional

import polars as pl
import pytest

from frameright import Field
from frameright.exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    TypeMismatchError,
)
from frameright.polars.eager import Col, Schema
from frameright.polars.lazy import Col as ColLazy
from frameright.polars.lazy import Schema as SchemaLazy

# ---------------------------------------------------------------------------
# Schema Definitions for Testing
# ---------------------------------------------------------------------------


class UserData(Schema):
    """Kitchen-sink schema covering all features."""

    user_id: Col[int] = Field(unique=True)
    username: Col[str] = Field(min_length=1)
    is_active: Col[bool]
    engagement_score: Col[float] = Field(ge=0.0, le=100.0)
    tier: Col[str] = Field(
        alias="SUBSCRIPTION_TIER", isin=["Free", "Pro", "Enterprise"]
    )
    lifetime_value: Optional[Col[float]] = Field(ge=0.0)


class UserDataLazy(SchemaLazy):
    """Kitchen-sink lazy schema covering all features."""

    user_id: ColLazy[int] = Field(unique=True)
    username: ColLazy[str] = Field(min_length=1)
    is_active: ColLazy[bool]
    engagement_score: ColLazy[float] = Field(ge=0.0, le=100.0)
    tier: ColLazy[str] = Field(
        alias="SUBSCRIPTION_TIER", isin=["Free", "Pro", "Enterprise"]
    )
    lifetime_value: Optional[ColLazy[float]] = Field(ge=0.0)


class StrictSchema(Schema):
    """Schema with strict non-nullable, unique constraints."""

    id: Col[int] = Field(unique=True, nullable=False)
    code: Col[str] = Field(regex=r"^[A-Z]{3}$", nullable=False)
    value: Col[float] = Field(gt=0, lt=1000)


class MinimalSchema(Schema):
    """Simplest possible schema."""

    col_a: Col[int]
    col_b: Col[str]


# For cross-backend tests
from frameright.pandas import Schema as PandasSchema  # noqa: E402


class MinimalSchemaPandas(PandasSchema):
    """Pandas version of minimal schema."""

    col_a: Col[int]
    col_b: Col[str]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_df() -> pl.DataFrame:
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
    def test_successful_initialization(self, valid_df: pl.DataFrame):
        """Valid Polars data creates a valid object."""
        users = UserData(valid_df)
        assert len(users) == 3
        assert users.fr_backend.name == "polars"

    def test_auto_detects_polars_backend(self, valid_df: pl.DataFrame):
        """Backend is auto-detected as 'polars'."""
        users = UserData(valid_df)
        assert users.fr_backend.name == "polars"

    def test_copy_flag_creates_independent_copy(self, valid_df: pl.DataFrame):
        """copy=True creates an independent copy."""
        users = UserData(valid_df, copy=True)
        # Polars clones on copy
        assert users.fr_data is not valid_df

    def test_validate_false_skips_validation(self):
        """validate=False skips all checks."""
        df = pl.DataFrame({"wrong": [1, 2, 3]})
        obj = MinimalSchema(df, validate=False)
        assert len(obj) == 3


# ===========================================================================
# SECTION 2: Missing Column Detection
# ===========================================================================


class TestPolarsMissingColumns:
    def test_missing_required_column(self, valid_df: pl.DataFrame):
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
    def test_int_dtype_mismatch(self, valid_df: pl.DataFrame):
        """String values in an int column raises TypeMismatchError."""
        df = valid_df.with_columns(pl.Series("user_id", ["1", "2", "3"]))
        with pytest.raises(TypeMismatchError, match="dtype"):
            UserData(df)

    def test_str_dtype_accepts_utf8(self, valid_df):
        """String columns accept Polars Utf8 type."""
        users = UserData(valid_df)
        assert users.fr_data["username"][0] == "alice"


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

    def test_gt_constraint(self, strict_df: pl.DataFrame):
        """gt (>) constraint rejects values at or below threshold."""
        df = strict_df.with_columns(pl.Series("value", [0.0, 500.0, 999.0]))
        with pytest.raises(ConstraintViolationError, match="greater_than"):
            StrictSchema(df)

    def test_lt_constraint(self, strict_df: pl.DataFrame):
        """lt (<) constraint rejects values at or above threshold."""
        df = strict_df.with_columns(pl.Series("value", [1000.0, 500.0, 999.0]))
        with pytest.raises(ConstraintViolationError, match="less_than"):
            StrictSchema(df)

    def test_isin_constraint(self, valid_df: pl.DataFrame):
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
        """Property getter returns Series for eager DataFrames (not Expr)."""
        users = UserData(valid_df)
        # For eager DataFrames, return Series for easier aggregations
        assert isinstance(users.user_id, pl.Series)
        assert users.user_id.to_list() == [1, 2, 3]

    def test_getter_series_supports_operations(self, valid_df):
        """Property getter Series supports direct operations."""
        users = UserData(valid_df)
        # Can call methods directly on the returned Series
        assert users.user_id.sum() == 6
        assert users.user_id.mean() == 2.0

    def test_alias_mapping(self, valid_df):
        """Aliased column maps correctly via expression."""
        users = UserData(valid_df)
        result = users.tier.to_list()
        assert result == ["Pro", "Free", "Enterprise"]

    def test_setter_with_series(self, valid_df):
        """Property setter with Series updates the underlying DataFrame."""
        users = UserData(valid_df, validate=False)
        users.user_id = pl.Series("user_id", [10, 20, 30])
        result = users.user_id.to_list()
        assert result == [10, 20, 30]

    def test_setter_with_expr(self, valid_df):
        """Property setter with Expr updates the underlying DataFrame."""
        users = UserData(valid_df, validate=False)
        users.engagement_score = users.engagement_score * 2
        result = users.fr_data["engagement_score"].to_list()
        assert result == [171.0, 24.0, 199.8]

    def test_setter_with_literal(self, valid_df):
        """Property setter with a scalar literal."""
        users = UserData(valid_df, validate=False)
        users.is_active = pl.Series("is_active", [True, True, True])
        assert users.fr_data["is_active"].to_list() == [True, True, True]


# ===========================================================================
# SECTION 6: Core Methods
# ===========================================================================


class TestPolarsCoreMethods:
    def test_fr_data_returns_polars_df(self, valid_df):
        """fr_data returns Polars DataFrame."""
        users = UserData(valid_df)
        assert isinstance(users.fr_data, pl.DataFrame)

    def test_filter_with_expr(self, valid_df):
        """Filtering works with pl.Expr from property getter."""
        users = UserData(valid_df)
        active = users.__class__(
            users.fr_data.filter(users.is_active), copy=False, validate=False
        )
        assert len(active) == 2
        assert isinstance(active, UserData)

    def test_filter_with_comparison_expr(self, valid_df):
        """Filtering works with comparison expressions."""
        users = UserData(valid_df)
        high = users.__class__(
            users.fr_data.filter(users.engagement_score > 50.0),
            copy=False,
            validate=False,
        )
        assert len(high) == 2

    def test_to_dict(self, valid_df):
        """to_dict returns a list of dicts."""
        users = UserData(valid_df)
        d = users.fr_data.to_dicts()
        assert isinstance(d, list)
        assert len(d) == 3

    def test_to_csv(self, valid_df, tmp_path):
        """to_csv writes a CSV file."""
        users = UserData(valid_df)
        path = str(tmp_path / "test.csv")
        users.fr_data.write_csv(path)
        loaded = pl.read_csv(path)
        assert loaded.height == 3


# ===========================================================================
# SECTION 7: Example Generation
# ===========================================================================


# ===========================================================================
# SECTION 8: Coercion
# ===========================================================================


class TestPolarsCoercion:
    def test_coerce_int_column(self):
        """Coerce string→int on Polars."""
        df = pl.DataFrame({"col_a": ["1", "2", "3"], "col_b": ["a", "b", "c"]})
        obj = MinimalSchema(df, coerce=True)
        assert obj.fr_data["col_a"].dtype == pl.Int64

    def test_coerce_bool_from_string(self):
        """Coerce string→bool on Polars."""

        class BoolSchema(Schema):
            flag: Col[bool]

        df = pl.DataFrame({"flag": ["true", "false", "yes"]})
        obj = BoolSchema(df, coerce=True)
        assert obj.fr_data["flag"].to_list() == [True, False, True]


# ===========================================================================
# SECTION 8.5: Strict Mode
# ===========================================================================


class TestPolarsStrict:
    def test_strict_false_allows_extra_columns(self):
        """strict=False (default) allows extra columns."""
        df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"], "extra": [10, 20]})
        obj = MinimalSchema(df, strict=False)
        assert "extra" in obj.fr_data.columns

    def test_strict_true_rejects_extra_columns(self):
        """strict=True rejects DataFrames with extra columns."""
        df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"], "extra": [10, 20]})
        from frameright.exceptions import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            MinimalSchema(df, strict=True)
        # Should mention the extra column or strict mode
        assert (
            "extra" in str(exc_info.value).lower()
            or "strict" in str(exc_info.value).lower()
        )

    def test_strict_true_accepts_exact_columns(self):
        """strict=True accepts DataFrames with exactly the schema columns."""
        df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        obj = MinimalSchema(df, strict=True)
        assert len(obj) == 2


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
        """fr_schema_info returns a list of dicts (backend-independent)."""
        info = MinimalSchema.fr_schema_info()
        assert isinstance(info, list)
        assert len(info) == 2


# ===========================================================================
# SECTION 11: Backend Registry
# ===========================================================================
# SECTION 12: Cross-backend consistency
# ===========================================================================


class TestCrossBackendConsistency:
    def test_same_schema_both_backends(self):
        """Same schema works with both Pandas and Polars data."""
        import pandas as pd

        pd_df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        pl_df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

        pd_obj = MinimalSchemaPandas(pd_df)
        pl_obj = MinimalSchema(pl_df)

        assert len(pd_obj) == len(pl_obj) == 2
        assert pd_obj.fr_backend.name == "pandas"
        assert pl_obj.fr_backend.name == "polars"

    def test_to_dict_consistent(self):
        """to_dict returns same structure for both backends."""
        import pandas as pd

        pd_df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        pl_df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

        pd_obj = MinimalSchemaPandas(pd_df)
        pl_obj = MinimalSchema(pl_df)

        pd_dict = pd_obj.fr_data.to_dict(orient="records")
        pl_dict = pl_obj.fr_data.to_dicts()

        assert pd_dict == pl_dict


# ===========================================================================
# SECTION 13: LazyFrame Support
# ===========================================================================


class TestPolarsLazyFrame:
    """Tests for Polars LazyFrame integration."""

    @pytest.fixture
    def lazy_df(self, valid_df):
        """Convert the valid DataFrame to a LazyFrame."""
        return valid_df.lazy()

    def test_lazyframe_initialization(self, valid_df: pl.LazyFrame):
        """Schema wraps a LazyFrame successfully."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        assert len(users) == 3

    def test_lazyframe_property_returns_expr(self, valid_df):
        """Property getter on LazyFrame returns pl.Expr."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        assert isinstance(users.user_id, pl.Expr)

    def test_lazyframe_filter_with_expr(self, valid_df: pl.DataFrame):
        """Filtering works on LazyFrame with expressions."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        active = users.__class__(
            users.fr_data.filter(users.is_active), copy=False, validate=False
        )
        assert isinstance(active.fr_data, pl.LazyFrame)
        # Collect to verify
        assert active.fr_data.collect().height == 2

    def test_lazyframe_filter_with_comparison(self, valid_df):
        """Comparison expressions work on LazyFrame filter."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        high = users.__class__(
            users.fr_data.filter(users.engagement_score > 50.0),
            copy=False,
            validate=False,
        )
        assert high.fr_data.collect().height == 2

    def test_lazyframe_setter_with_expr(self, valid_df):
        """Setter with expression on LazyFrame."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf, validate=False)
        users.engagement_score = users.engagement_score * 2
        result = users.fr_data.collect()["engagement_score"].to_list()
        assert result == [171.0, 24.0, 199.8]

    def test_lazyframe_setter_with_literal(self, valid_df: pl.DataFrame):
        """Setter with literal value on LazyFrame."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf, validate=False)
        users.is_active = pl.lit(True)  # Use pl.lit() for lazy frames
        result = users.fr_data.collect()["is_active"].to_list()
        assert result == [True, True, True]

    def test_lazyframe_to_dict(self, valid_df: pl.DataFrame):
        """to_dict collects LazyFrame before converting."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        d = users.fr_data.collect().to_dicts()
        assert isinstance(d, list)
        assert len(d) == 3

    def test_lazyframe_to_csv(self, valid_df, tmp_path):
        """to_csv collects LazyFrame before writing."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        path = str(tmp_path / "lazy_test.csv")
        users.fr_data.collect().write_csv(path)
        loaded = pl.read_csv(path)
        assert loaded.height == 3

    def test_lazyframe_copy_returns_lazyframe(self, valid_df):
        """copy on LazyFrame returns the same LazyFrame (immutable)."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf, copy=True)
        assert isinstance(users.fr_data, pl.LazyFrame)

    def test_lazyframe_head(self, valid_df):
        """head collects LazyFrame to return DataFrame."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        h = users.fr_backend.head(users.fr_data, 2)
        assert isinstance(h, pl.DataFrame)
        assert h.height == 2

    def test_lazyframe_repr(self, valid_df):
        """repr works with LazyFrame."""
        lf = valid_df.lazy()
        users = UserDataLazy(lf)
        r = repr(users)
        assert "UserDataLazy" in r
        assert "polars" in r


# ===========================================================================
# SECTION 14: Expression Chaining
# ===========================================================================


class TestExpressionChaining:
    """Tests for pl.Expr-based property composition."""

    def test_arithmetic_expression(self, valid_df):
        """Arithmetic on property Series produces valid results."""
        users = UserData(valid_df, validate=False)
        result = users.engagement_score * 2 + 1
        # Eager DataFrames return Series, arithmetic returns Series
        assert isinstance(result, pl.Series)
        assert result.to_list() == [172.0, 25.0, 200.8]

    def test_comparison_expression(self, valid_df):
        """Comparison on property Series produces boolean Series."""
        users = UserData(valid_df)
        mask = users.engagement_score > 50.0
        # Eager DataFrames return Series, comparison returns boolean Series
        assert isinstance(mask, pl.Series)
        assert mask.to_list() == [True, False, True]

    def test_string_expression(self, valid_df):
        """String methods on property Series work correctly."""
        users = UserData(valid_df)
        result = users.username.str.to_uppercase()
        # Eager DataFrames return Series, string methods return Series
        assert isinstance(result, pl.Series)
        assert result.to_list() == ["ALICE", "BOB", "CHARLIE"]

    def test_with_columns_using_property(self, valid_df):
        """df.with_columns() using property Series."""
        users = UserData(valid_df, validate=False)
        # PolarsBackend returns native pl.Series directly
        new_df = valid_df.with_columns(
            (users.engagement_score / 100.0).alias("score_pct"),
        )
        assert "score_pct" in new_df.columns
        assert new_df["score_pct"].to_list() == pytest.approx([0.855, 0.12, 0.999])

    def test_select_multiple_properties(self, valid_df):
        """Selecting multiple property Series."""
        users = UserData(valid_df)
        # PolarsBackend returns native pl.Series directly
        subset = valid_df.select(users.user_id, users.username)
        assert subset.columns == ["user_id", "username"]
        assert subset.height == 3

    @pytest.fixture
    def valid_df(self):
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
