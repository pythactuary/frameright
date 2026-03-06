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
        assert users.sf_data["username"][0] == "alice"


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
    def test_getter_returns_expr(self, valid_df):
        """Property getter returns a lazy pl.Expr (pl.col())."""
        users = UserData(valid_df)
        assert isinstance(users.user_id, pl.Expr)

    def test_getter_expr_resolves_correctly(self, valid_df):
        """Property getter expression resolves to correct data when selected."""
        users = UserData(valid_df)
        result = users.sf_data.select(users.user_id).to_series().to_list()
        assert result == [1, 2, 3]

    def test_alias_mapping(self, valid_df):
        """Aliased column maps correctly via expression."""
        users = UserData(valid_df)
        result = users.sf_data.select(users.tier).to_series().to_list()
        assert result == ["Pro", "Free", "Enterprise"]

    def test_setter_with_series(self, valid_df):
        """Property setter with Series updates the underlying DataFrame."""
        users = UserData(valid_df, validate=False)
        users.user_id = pl.Series("user_id", [10, 20, 30])
        result = users.sf_data.select(users.user_id).to_series().to_list()
        assert result == [10, 20, 30]

    def test_setter_with_expr(self, valid_df):
        """Property setter with Expr updates the underlying DataFrame."""
        users = UserData(valid_df, validate=False)
        users.engagement_score = pl.col("engagement_score") * 2
        result = users.sf_data["engagement_score"].to_list()
        assert result == [171.0, 24.0, 199.8]

    def test_setter_with_literal(self, valid_df):
        """Property setter with a scalar literal."""
        users = UserData(valid_df, validate=False)
        users.is_active = True
        assert users.sf_data["is_active"].to_list() == [True, True, True]


# ===========================================================================
# SECTION 6: Core Methods
# ===========================================================================


class TestPolarsCoreMethods:
    def test_sf_data_returns_polars_df(self, valid_df):
        """sf_data returns the underlying Polars DataFrame."""
        users = UserData(valid_df)
        assert isinstance(users.sf_data, pl.DataFrame)

    def test_sf_filter_with_expr(self, valid_df):
        """sf_filter works with pl.Expr from property getter."""
        users = UserData(valid_df)
        active = users.sf_filter(users.is_active)
        assert len(active) == 2
        assert isinstance(active, UserData)

    def test_sf_filter_with_comparison_expr(self, valid_df):
        """sf_filter works with comparison expressions."""
        users = UserData(valid_df)
        high = users.sf_filter(users.engagement_score > 50.0)
        assert len(high) == 2

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
        assert obj.sf_data["flag"].to_list() == [True, False, True]


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


# ===========================================================================
# SECTION 13: LazyFrame Support
# ===========================================================================


class TestPolarsLazyFrame:
    """Tests for Polars LazyFrame integration."""

    @pytest.fixture
    def lazy_df(self, valid_df):
        """Convert the valid DataFrame to a LazyFrame."""
        return valid_df.lazy()

    def test_lazyframe_auto_detection(self):
        """LazyFrame is auto-detected as polars backend."""
        from structframe import detect_backend

        lf = pl.LazyFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        adapter = detect_backend(lf)
        assert adapter.name == "polars"

    def test_lazyframe_initialization(self, valid_df):
        """StructFrame wraps a LazyFrame successfully."""
        lf = valid_df.lazy()
        users = UserData(lf)
        assert len(users) == 3

    def test_lazyframe_property_returns_expr(self, valid_df):
        """Property getter on LazyFrame returns pl.Expr."""
        lf = valid_df.lazy()
        users = UserData(lf)
        assert isinstance(users.user_id, pl.Expr)

    def test_lazyframe_filter_with_expr(self, valid_df):
        """sf_filter works on LazyFrame with expressions."""
        lf = valid_df.lazy()
        users = UserData(lf)
        active = users.sf_filter(users.is_active)
        assert isinstance(active.sf_data, pl.LazyFrame)
        # Collect to verify
        assert active.sf_data.collect().height == 2

    def test_lazyframe_filter_with_comparison(self, valid_df):
        """Comparison expressions work on LazyFrame filter."""
        lf = valid_df.lazy()
        users = UserData(lf)
        high = users.sf_filter(users.engagement_score > 50.0)
        assert high.sf_data.collect().height == 2

    def test_lazyframe_setter_with_expr(self, valid_df):
        """Setter with expression on LazyFrame."""
        lf = valid_df.lazy()
        users = UserData(lf, validate=False)
        users.engagement_score = pl.col("engagement_score") * 2
        result = users.sf_data.collect()["engagement_score"].to_list()
        assert result == [171.0, 24.0, 199.8]

    def test_lazyframe_setter_with_literal(self, valid_df):
        """Setter with literal value on LazyFrame."""
        lf = valid_df.lazy()
        users = UserData(lf, validate=False)
        users.is_active = True
        result = users.sf_data.collect()["is_active"].to_list()
        assert result == [True, True, True]

    def test_lazyframe_to_dict(self, valid_df):
        """to_dict collects LazyFrame before converting."""
        lf = valid_df.lazy()
        users = UserData(lf)
        d = users.sf_to_dict(orient="records")
        assert isinstance(d, list)
        assert len(d) == 3

    def test_lazyframe_to_csv(self, valid_df, tmp_path):
        """to_csv collects LazyFrame before writing."""
        lf = valid_df.lazy()
        users = UserData(lf)
        path = str(tmp_path / "lazy_test.csv")
        users.sf_to_csv(path)
        loaded = pl.read_csv(path)
        assert loaded.height == 3

    def test_lazyframe_copy_returns_lazyframe(self, valid_df):
        """copy on LazyFrame returns the same LazyFrame (immutable)."""
        lf = valid_df.lazy()
        users = UserData(lf, copy=True)
        assert isinstance(users.sf_data, pl.LazyFrame)

    def test_lazyframe_head(self, valid_df):
        """head collects LazyFrame to return DataFrame."""
        lf = valid_df.lazy()
        users = UserData(lf)
        h = users.sf_backend.head(users.sf_data, 2)
        assert isinstance(h, pl.DataFrame)
        assert h.height == 2

    def test_lazyframe_repr(self, valid_df):
        """repr works with LazyFrame."""
        lf = valid_df.lazy()
        users = UserData(lf)
        r = repr(users)
        assert "UserData" in r
        assert "polars" in r


# ===========================================================================
# SECTION 14: sf_collect() — LazyFrame materialisation
# ===========================================================================


class TestSfCollect:
    """Tests for the sf_collect() materialisation escape hatch."""

    def test_collect_lazyframe_returns_dataframe(self, valid_df):
        """sf_collect() materialises a LazyFrame into an eager StructFrame."""
        lf = valid_df.lazy()
        users = UserData(lf)
        assert isinstance(users.sf_data, pl.LazyFrame)

        collected = users.sf_collect()
        assert isinstance(collected, UserData)
        assert isinstance(collected.sf_data, pl.DataFrame)
        assert collected.sf_data.height == 3

    def test_collect_eager_returns_self(self, valid_df):
        """sf_collect() on an eager DataFrame returns self unchanged."""
        users = UserData(valid_df)
        same = users.sf_collect()
        assert same is users  # identity — no copy needed

    def test_collect_preserves_data(self, valid_df):
        """Collected data matches the original."""
        lf = valid_df.lazy()
        lazy_users = UserData(lf)
        eager_users = lazy_users.sf_collect()
        assert eager_users.sf_data.to_dicts() == valid_df.to_dicts()

    def test_collect_then_access_series(self, valid_df):
        """After collect, get_column returns an actual pl.Series."""
        lf = valid_df.lazy()
        users = UserData(lf).sf_collect()
        series = users.sf_backend.get_column(users.sf_data, "user_id")
        assert isinstance(series, pl.Series)
        assert series.to_list() == [1, 2, 3]

    def test_collect_after_filter(self, valid_df):
        """Collect works after a lazy filter."""
        lf = valid_df.lazy()
        users = UserData(lf)
        active = users.sf_filter(users.is_active)
        assert isinstance(active.sf_data, pl.LazyFrame)

        collected = active.sf_collect()
        assert isinstance(collected.sf_data, pl.DataFrame)
        assert collected.sf_data.height == 2

    def test_collect_pandas_noop(self):
        """sf_collect() is a no-op for Pandas backend."""
        import pandas as pd

        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["a", "b"]})
        obj = MinimalSchema(df)
        same = obj.sf_collect()
        assert same is obj

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


# ===========================================================================
# SECTION 15: Expression Chaining
# ===========================================================================


class TestExpressionChaining:
    """Tests for pl.Expr-based property composition."""

    def test_arithmetic_expression(self, valid_df):
        """Arithmetic on property Exprs produces valid expressions."""
        users = UserData(valid_df, validate=False)
        expr = users.engagement_score * 2 + 1
        assert isinstance(expr, pl.Expr)
        result = valid_df.select(expr.alias("result")).to_series().to_list()
        assert result == [172.0, 25.0, 200.8]

    def test_comparison_expression(self, valid_df):
        """Comparison on property Exprs produces filter-ready expressions."""
        users = UserData(valid_df)
        expr = users.engagement_score > 50.0
        assert isinstance(expr, pl.Expr)
        filtered = valid_df.filter(expr)
        assert filtered.height == 2

    def test_string_expression(self, valid_df):
        """String methods on property Exprs work."""
        users = UserData(valid_df)
        expr = users.username.str.to_uppercase()
        assert isinstance(expr, pl.Expr)
        result = valid_df.select(expr.alias("upper")).to_series().to_list()
        assert result == ["ALICE", "BOB", "CHARLIE"]

    def test_with_columns_using_property(self, valid_df):
        """df.with_columns() using property Exprs."""
        users = UserData(valid_df, validate=False)
        new_df = valid_df.with_columns(
            (users.engagement_score / 100.0).alias("score_pct"),
        )
        assert "score_pct" in new_df.columns
        assert new_df["score_pct"].to_list() == pytest.approx([0.855, 0.12, 0.999])

    def test_select_multiple_properties(self, valid_df):
        """Selecting multiple property Exprs."""
        users = UserData(valid_df)
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
