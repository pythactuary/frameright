from datetime import date, datetime
from typing import Optional

import pandas as pd
import pytest

from frameright import Field, FieldInfo
from frameright.exceptions import (
    ConstraintViolationError,
    MissingColumnError,
    SchemaError,
    StructFrameError,
    TypeMismatchError,
    ValidationError,
)
from frameright.pandas import Col, Index, Schema

# ---------------------------------------------------------------------------
# Schema Definitions for Testing
# ---------------------------------------------------------------------------


class UserData(Schema):
    """Kitchen-sink schema covering all features."""

    user_id: Col[int] = Field(unique=True)
    username: Col[str] = Field(min_length=1)
    is_active: Col[bool] = Field(nullable=False)
    engagement_score: Col[float] = Field(ge=0.0, le=100.0)
    tier: Col[str] = Field(
        alias="SUBSCRIPTION_TIER", isin=["Free", "Pro", "Enterprise"]
    )
    lifetime_value: Optional[Col[float]] = Field(ge=0.0)


class StrictSchema(Schema):
    """Schema with strict non-nullable, unique constraints."""

    id: Col[int] = Field(unique=True, nullable=False)
    code: Col[str] = Field(regex=r"^[A-Z]{3}$", nullable=False)
    value: Col[float] = Field(gt=0, lt=1000)


class MinimalSchema(Schema):
    """Simplest possible schema."""

    col_a: Col[int]
    col_b: Col[str]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_df():
    """Provides a perfectly valid, clean dataframe."""
    return pd.DataFrame(
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
    return valid_df.drop(columns=["lifetime_value"])


@pytest.fixture
def strict_df():
    """Provides a valid dataframe for StrictSchema."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "code": ["ABC", "DEF", "GHI"],
            "value": [10.0, 500.0, 999.0],
        }
    )


# ===========================================================================
# SECTION 1: Initialization & Basic Properties
# ===========================================================================


class TestInitialization:
    def test_successful_initialization(self, valid_df):
        """Valid data creates a valid object with correct properties."""
        users = UserData(valid_df)
        assert len(users) == 3
        assert users.tier[0] == "Pro"
        assert not users.is_active[1]

    def test_copy_flag_creates_independent_copy(self, valid_df):
        """copy=True should create an independent copy of the data."""
        users = UserData(valid_df, copy=True)
        valid_df.loc[0, "user_id"] = 999
        assert users.user_id[0] == 1  # unaffected

    def test_no_copy_shares_data(self, valid_df):
        """copy=False (default) should share the underlying data."""
        users = UserData(valid_df, copy=False)
        valid_df.loc[0, "username"] = "modified"
        assert users.username[0] == "modified"

    def test_validate_false_skips_validation(self):
        """validate=False should skip all validation checks."""
        bad_df = pd.DataFrame({"wrong_col": [1]})
        obj = MinimalSchema(bad_df, validate=False)
        assert len(obj) == 1

    def test_validate_types_false_skips_dtype_check(self):
        """validate_types=False should skip dtype checks but still check columns."""
        df = pd.DataFrame({"col_a": ["1", "2"], "col_b": ["a", "b"]})
        # col_a is string but annotated as int; should pass with validate_types=False
        obj = MinimalSchema(df, validate_types=False)
        assert len(obj) == 2


# ===========================================================================
# SECTION 2: Missing Column Validation
# ===========================================================================


class TestMissingColumns:
    def test_missing_required_column_raises(self, valid_df):
        """Missing REQUIRED columns raise MissingColumnError."""
        bad_df = valid_df.drop(columns=["username"])
        with pytest.raises(MissingColumnError, match="username"):
            UserData(bad_df)

    def test_missing_optional_column_is_safe(self, valid_df_missing_optional):
        """Missing OPTIONAL columns do not raise and return None."""
        users = UserData(valid_df_missing_optional)
        assert len(users) == 3
        assert users.lifetime_value is None

    def test_multiple_missing_columns(self):
        """All missing required columns should be reported."""
        df = pd.DataFrame({"engagement_score": [50.0]})
        with pytest.raises(MissingColumnError):
            UserData(df)


# ===========================================================================
# SECTION 3: Runtime Dtype Validation
# ===========================================================================


class TestDtypeValidation:
    def test_int_dtype_mismatch(self, valid_df):
        """Strings in an int column raises TypeMismatchError."""
        valid_df["user_id"] = ["1", "2", "3"]
        with pytest.raises(TypeMismatchError, match="dtype"):
            UserData(valid_df)

    def test_float_dtype_mismatch(self, valid_df):
        """Strings in a float column raises TypeMismatchError."""
        valid_df["engagement_score"] = ["85.5", "12.0", "99.9"]
        with pytest.raises(TypeMismatchError, match="dtype"):
            UserData(valid_df)

    def test_bool_dtype_mismatch(self, valid_df):
        """Strings in a bool column raises TypeMismatchError."""
        valid_df["is_active"] = ["yes", "no", "yes"]
        with pytest.raises(TypeMismatchError, match="dtype"):
            UserData(valid_df)

    def test_str_dtype_accepts_object(self, valid_df):
        """String columns should accept object dtype (the pandas default)."""
        # This is the happy path - object dtype is valid for str
        users = UserData(valid_df)
        assert users.username[0] == "alice"


# ===========================================================================
# SECTION 4: Field-Level Constraint Validation
# ===========================================================================


class TestFieldConstraints:
    def test_ge_constraint(self, valid_df):
        """ge (>=) constraint rejects values below threshold."""
        valid_df.loc[0, "engagement_score"] = -1.0
        with pytest.raises(ConstraintViolationError, match="greater_than_or_equal_to"):
            UserData(valid_df)

    def test_le_constraint(self, valid_df):
        """le (<=) constraint rejects values above threshold."""
        valid_df.loc[0, "engagement_score"] = 150.0
        with pytest.raises(ConstraintViolationError, match="less_than_or_equal_to"):
            UserData(valid_df)

    def test_gt_constraint(self, strict_df):
        """gt (>) constraint rejects values at or below threshold."""
        strict_df.loc[0, "value"] = 0.0  # must be > 0
        with pytest.raises(ConstraintViolationError, match="greater_than"):
            StrictSchema(strict_df)

    def test_lt_constraint(self, strict_df):
        """lt (<) constraint rejects values at or above threshold."""
        strict_df.loc[0, "value"] = 1000.0  # must be < 1000
        with pytest.raises(ConstraintViolationError, match="less_than"):
            StrictSchema(strict_df)

    def test_isin_constraint(self, valid_df):
        """isin constraint rejects values not in allowed list."""
        valid_df.loc[1, "SUBSCRIPTION_TIER"] = "SuperPro"
        with pytest.raises(ConstraintViolationError, match="isin"):
            UserData(valid_df)

    def test_regex_constraint(self, strict_df):
        """regex constraint rejects values not matching pattern."""
        strict_df.loc[0, "code"] = "abc"  # must be ^[A-Z]{3}$
        with pytest.raises(ConstraintViolationError, match="str_matches"):
            StrictSchema(strict_df)

    def test_regex_constraint_valid(self, strict_df):
        """regex constraint accepts matching values."""
        obj = StrictSchema(strict_df)
        assert obj.code[0] == "ABC"

    def test_min_length_constraint(self, valid_df):
        """min_length constraint rejects values that are too short."""
        valid_df.loc[0, "username"] = ""  # min_length=1
        with pytest.raises(ConstraintViolationError, match="str_length"):
            UserData(valid_df)

    def test_nullable_false_constraint(self, strict_df):
        """nullable=False rejects NaN values."""
        strict_df.loc[0, "code"] = None
        with pytest.raises(ConstraintViolationError, match="null"):
            StrictSchema(strict_df)

    def test_unique_constraint(self, strict_df):
        """unique=True rejects duplicate values."""
        strict_df.loc[1, "id"] = strict_df.loc[0, "id"]  # duplicate
        with pytest.raises(ConstraintViolationError, match="field_uniqueness"):
            StrictSchema(strict_df)

    def test_optional_column_constraint_when_present(self, valid_df):
        """Constraints on optional columns still apply when the column exists."""
        valid_df.loc[0, "lifetime_value"] = -10.0
        with pytest.raises(ConstraintViolationError, match="greater_than_or_equal_to"):
            UserData(valid_df)


# ===========================================================================
# SECTION 5: Property Getters & Setters
# ===========================================================================


class TestProperties:
    def test_getter_returns_series(self, valid_df):
        """Property getter returns a pandas Series."""
        users = UserData(valid_df)
        assert isinstance(users.username, pd.Series)

    def test_setter_updates_dataframe(self, valid_df):
        """Property setter modifies the underlying DataFrame."""
        users = UserData(valid_df)
        users.engagement_score = users.engagement_score + 10.0
        assert users.engagement_score[0] == 95.5

    def test_alias_property(self, valid_df):
        """Aliased properties access the correct DataFrame column."""
        users = UserData(valid_df)
        assert users.tier[0] == "Pro"
        assert "SUBSCRIPTION_TIER" in users.fr_data.columns


# ===========================================================================
# SECTION 6: Core Methods (fr_ prefixed)
# ===========================================================================


class TestCoreMethods:
    def test_fr_data_returns_dataframe(self, valid_df):
        """fr_data returns the underlying DataFrame."""
        users = UserData(valid_df)
        assert isinstance(users.fr_data, pd.DataFrame)

    def test_fr_validate_returns_self(self, valid_df):
        """fr_validate returns self for method chaining."""
        users = UserData(valid_df, validate=False)
        result = users.fr_validate()
        assert result is users


# ===========================================================================
# SECTION 7: Example Data Generation
# ===========================================================================


class TestCoercion:
    def test_coerce_strings_to_int(self):
        """Constructor with coerce=True converts string columns to match int annotation."""
        df = pd.DataFrame({"col_a": ["1", "2", "3"], "col_b": ["a", "b", "c"]})
        obj = MinimalSchema(df, coerce=True)
        assert isinstance(obj, MinimalSchema)
        assert obj.col_a[0] == 1

    def test_coerce_preserves_valid_types(self):
        """Coercion doesn't break columns that already have the right dtype."""
        df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["a", "b", "c"]})
        obj = MinimalSchema(df, coerce=True)
        assert len(obj) == 3

    def test_coerce_raises_on_invalid(self):
        """Coercion raises TypeError when conversion is impossible."""
        df = pd.DataFrame({"col_a": ["not_a_number"], "col_b": ["a"]})
        with pytest.raises(TypeError, match="coerce"):
            MinimalSchema(df, coerce=True)


# ===========================================================================
# SECTION 9: Schema Introspection
# ===========================================================================


class TestSchemaIntrospection:
    def test_fr_schema_info_returns_list(self):
        """fr_schema_info returns a list of dicts describing the schema."""
        info = UserData.fr_schema_info()
        assert isinstance(info, list)
        assert len(info) > 0
        assert isinstance(info[0], dict)
        assert "attribute" in info[0]
        assert "column" in info[0]
        assert "type" in info[0]
        assert "required" in info[0]

    def test_fr_schema_info_correct_content(self):
        """fr_schema_info contains correct schema details."""
        info = UserData.fr_schema_info()
        tier_row = next(r for r in info if r["attribute"] == "tier")
        assert tier_row["column"] == "SUBSCRIPTION_TIER"
        assert tier_row["required"]

    def test_fr_schema_info_shows_optional(self):
        """fr_schema_info correctly marks optional columns."""
        info = UserData.fr_schema_info()
        ltv_row = next(r for r in info if r["attribute"] == "lifetime_value")
        assert not ltv_row["required"]


# ===========================================================================
# SECTION 10: Python Protocols
# ===========================================================================


class TestPythonProtocols:
    def test_len(self, valid_df):
        """__len__ returns the number of rows."""
        users = UserData(valid_df)
        assert len(users) == 3

    def test_repr(self, valid_df):
        """__repr__ contains class name and dimension info."""
        users = UserData(valid_df)
        r = repr(users)
        assert "UserData" in r
        assert "3 rows" in r

    def test_iter(self, valid_df):
        """__iter__ allows iterating over rows."""
        users = UserData(valid_df)
        rows = list(users)
        assert len(rows) == 3

    def test_eq_same_data(self, valid_df):
        """__eq__ returns True for identical data."""
        a = UserData(valid_df, copy=True)
        b = UserData(valid_df, copy=True)
        assert a == b

    def test_eq_different_data(self, valid_df):
        """__eq__ returns False for different data."""
        a = UserData(valid_df, copy=True)
        b = UserData(valid_df, copy=True)
        b.engagement_score = b.engagement_score + 1.0
        assert a != b

    def test_eq_different_type(self, valid_df):
        """__eq__ returns NotImplemented for different types."""
        users = UserData(valid_df)
        assert users.__eq__("not a frameright") is NotImplemented

    def test_contains(self, valid_df):
        """__contains__ supports 'col in obj' syntax."""
        users = UserData(valid_df)
        assert "username" in users
        assert "nonexistent" not in users


# ---------------------------------------------------------------------------
# 11. Index[T] support
# ---------------------------------------------------------------------------


class IndexedSchema(Schema):
    """Schema with an Index[T] annotation."""

    row_id: Index[int]
    name: Col[str]
    value: Col[float]


class TestIndexType:
    """Tests for the Index[T] feature."""

    @pytest.fixture()
    def indexed_df(self):
        df = pd.DataFrame({"name": ["Alice", "Bob", "Carol"], "value": [1.0, 2.0, 3.0]})
        df.index = pd.Index([10, 20, 30], name="row_id")
        return df

    def test_index_getter(self, indexed_df):
        """Index[T] attribute returns the DataFrame index."""
        obj = IndexedSchema(indexed_df, validate=False)
        pd.testing.assert_index_equal(obj.row_id, indexed_df.index)

    def test_index_setter(self, indexed_df):
        """Index[T] attribute can be set to update the DataFrame index."""
        obj = IndexedSchema(indexed_df, validate=False)
        new_idx = pd.Index([100, 200, 300])
        obj.row_id = new_idx
        pd.testing.assert_index_equal(obj.fr_data.index, new_idx)

    def test_index_not_in_schema(self, indexed_df):
        """Index[T] attribute should NOT appear in _fr_schema."""
        assert "row_id" not in IndexedSchema._fr_schema

    def test_index_does_not_interfere_with_validation(self, indexed_df):
        """Validation should only check Col columns, not Index."""
        obj = IndexedSchema(indexed_df, validate=True)
        assert len(obj) == 3


# ---------------------------------------------------------------------------
# 12. MultiIndex support
# ---------------------------------------------------------------------------


class MultiIndexSchema(Schema):
    """Schema with two Index[T] annotations for MultiIndex."""

    a: Index[int]
    b: Index[str]
    c: Col[float]


class TestMultiIndex:
    """Tests for MultiIndex support via multiple Index[T] annotations."""

    @pytest.fixture()
    def multi_df(self):
        df = pd.DataFrame({"c": [1.0, 2.0, 3.0]})
        df.index = pd.MultiIndex.from_arrays(
            [[10, 20, 30], ["x", "y", "z"]], names=["a", "b"]
        )
        return df

    def test_multi_index_not_in_schema(self):
        """Index attributes should not appear in _fr_schema."""
        assert "a" not in MultiIndexSchema._fr_schema
        assert "b" not in MultiIndexSchema._fr_schema
        assert "c" in MultiIndexSchema._fr_schema

    def test_multi_index_getter_level_a(self, multi_df):
        """Getter for first index level returns correct values."""
        obj = MultiIndexSchema(multi_df, validate=False)
        expected = pd.Index([10, 20, 30], name="a")
        pd.testing.assert_index_equal(obj.a, expected)

    def test_multi_index_getter_level_b(self, multi_df):
        """Getter for second index level returns correct values."""
        obj = MultiIndexSchema(multi_df, validate=False)
        expected = pd.Index(["x", "y", "z"], name="b")
        pd.testing.assert_index_equal(obj.b, expected)

    def test_multi_index_setter(self, multi_df):
        """Setter replaces one level of the MultiIndex."""
        obj = MultiIndexSchema(multi_df, validate=False)
        obj.a = pd.Index([100, 200, 300])
        result = obj.fr_data.index.get_level_values("a")
        pd.testing.assert_index_equal(result, pd.Index([100, 200, 300], name="a"))
        # Other level unchanged
        result_b = obj.fr_data.index.get_level_values("b")
        pd.testing.assert_index_equal(result_b, pd.Index(["x", "y", "z"], name="b"))

    def test_multi_index_col_access(self, multi_df):
        """Col columns still work alongside MultiIndex."""
        obj = MultiIndexSchema(multi_df, validate=False)
        pd.testing.assert_series_equal(obj.c, multi_df["c"])

    def test_multi_index_validation_ignores_indexes(self, multi_df):
        """Validation only checks Col columns, not Index annotations."""
        obj = MultiIndexSchema(multi_df, validate=True)
        assert len(obj) == 3


# ===========================================================================
# SECTION 13: Additional Schema Definitions
# ===========================================================================


class MaxLengthSchema(Schema):
    """Schema with max_length constraint."""

    label: Col[str] = Field(max_length=5)


class DateTimeSchema(Schema):
    """Schema with datetime and date columns."""

    event_time: Col[datetime]
    event_date: Col[date]
    label: Col[str]


class DescribedSchema(Schema):
    """Schema using docstring descriptions."""

    amount: Col[float] = Field(ge=0)
    """Transaction amount in USD"""
    currency: Col[str] = Field(isin=["USD", "EUR", "GBP"])
    """ISO currency code"""


class UnionColSchema(Schema):
    """Schema with Optional column."""

    name: Optional[Col[str]]
    value: Col[float]


class PrivateAttrSchema(Schema):
    """Schema that also has a private attribute."""

    _internal: int = 42
    col_a: Col[int]
    col_b: Col[str]


class EmptySchema(Schema):
    """Schema with no columns defined."""

    pass


class ParentSchema(Schema):
    """Base schema for inheritance testing."""

    id: Col[int] = Field(unique=True)
    name: Col[str]


class ChildSchema(ParentSchema):
    """Extended schema inheriting from ParentSchema."""

    score: Col[float] = Field(ge=0)


class AllTypesSchema(Schema):
    """Schema with every supported dtype."""

    i: Col[int]
    f: Col[float]
    s: Col[str]
    b: Col[bool] = Field(nullable=False)
    dt: Col[datetime]


# ===========================================================================
# SECTION 14: FieldInfo & Field()
# ===========================================================================


class TestFieldInfo:
    def test_field_returns_fieldinfo(self):
        """Field() returns a FieldInfo instance."""
        fi = Field(ge=0, le=100)
        assert isinstance(fi, FieldInfo)

    def test_fieldinfo_repr_with_constraints(self):
        """FieldInfo repr shows non-default parameters."""
        fi = FieldInfo(ge=0.0, le=100.0, nullable=False)
        r = repr(fi)
        assert "ge=0.0" in r
        assert "le=100.0" in r
        assert "nullable=False" in r

    def test_fieldinfo_repr_defaults(self):
        """FieldInfo repr is clean when no constraints set."""
        fi = FieldInfo()
        assert repr(fi) == "Field()"

    def test_fieldinfo_repr_alias(self):
        """FieldInfo repr includes alias."""
        fi = FieldInfo(alias="MY_COL")
        assert "alias='MY_COL'" in repr(fi)

    def test_fieldinfo_repr_unique_true(self):
        """FieldInfo repr includes unique when True."""
        fi = FieldInfo(unique=True)
        assert "unique=True" in repr(fi)


# ===========================================================================
# SECTION 15: max_length Constraint
# ===========================================================================


class TestMaxLengthConstraint:
    def test_max_length_rejects_long_values(self):
        """max_length rejects strings longer than threshold."""
        df = pd.DataFrame({"label": ["short", "toolongvalue"]})
        with pytest.raises(ConstraintViolationError, match="str_length"):
            MaxLengthSchema(df)

    def test_max_length_accepts_valid(self):
        """max_length accepts strings within limit."""
        df = pd.DataFrame({"label": ["hi", "hello"]})
        obj = MaxLengthSchema(df)
        assert len(obj) == 2


# ===========================================================================
# SECTION 16: DateTime / Date Dtype Validation
# ===========================================================================


class TestDateTimeDtype:
    def test_datetime_dtype_valid(self):
        """datetime columns accept datetime64 dtype."""
        df = pd.DataFrame(
            {
                "event_time": pd.to_datetime(["2024-01-01", "2024-06-15"]),
                "event_date": pd.to_datetime(["2024-01-01", "2024-06-15"]),
                "label": ["a", "b"],
            }
        )
        obj = DateTimeSchema(df)
        assert len(obj) == 2

    def test_datetime_dtype_mismatch(self):
        """String in a datetime column raises TypeMismatchError."""
        df = pd.DataFrame(
            {
                "event_time": ["not-a-date", "also-not"],
                "event_date": pd.to_datetime(["2024-01-01", "2024-06-15"]),
                "label": ["a", "b"],
            }
        )
        with pytest.raises(TypeMismatchError, match="datetime"):
            DateTimeSchema(df)


# ===========================================================================
# SECTION 17: Union Inside Col
# ===========================================================================


class TestUnionCol:
    def test_union_col_nullable_str(self):
        """Col[Union[str, None]] correctly resolves inner type to str."""
        schema = UnionColSchema._fr_schema
        assert schema["name"]["inner_type"] is str

    def test_union_col_validates(self):
        """Col[Union[str, None]] validates correctly."""
        df = pd.DataFrame({"name": ["alice", "bob"], "value": [1.0, 2.0]})
        obj = UnionColSchema(df)
        assert obj.name is not None
        assert obj.name[0] == "alice"


# ===========================================================================
# SECTION 18: Private Attributes Skipped
# ===========================================================================


class TestPrivateAttributes:
    def test_private_attrs_not_in_schema(self):
        """Attributes starting with _ should not be in _fr_schema."""
        assert "_internal" not in PrivateAttrSchema._fr_schema
        assert "col_a" in PrivateAttrSchema._fr_schema

    def test_private_attrs_preserved(self):
        """Private class attributes remain accessible."""
        assert PrivateAttrSchema._internal == 42


# ===========================================================================
# SECTION 19: Empty DataFrame & Empty Schema
# ===========================================================================


class TestEdgeCases:
    def test_empty_dataframe(self):
        """Empty DataFrame (0 rows) is valid if columns exist."""
        df = pd.DataFrame(
            {"col_a": pd.Series([], dtype="int64"), "col_b": pd.Series([], dtype="str")}
        )
        obj = MinimalSchema(df)
        assert len(obj) == 0

    def test_single_row(self, valid_df):
        """Single-row DataFrame works correctly."""
        df = valid_df.head(1)
        users = UserData(df)
        assert len(users) == 1

    def test_empty_schema(self):
        """Schema with no columns defined accepts any DataFrame."""
        df = pd.DataFrame({"anything": [1, 2, 3]})
        obj = EmptySchema(df)
        assert len(obj) == 3

    def test_extra_columns_ignored(self):
        """Extra columns in the DataFrame beyond the schema are allowed."""
        df = pd.DataFrame({"col_a": [1], "col_b": ["x"], "extra": [99]})
        obj = MinimalSchema(df)
        assert "extra" in obj.fr_data.columns

    def test_strict_false_allows_extra_columns(self):
        """strict=False (default) allows extra columns."""
        df = pd.DataFrame({"col_a": [1], "col_b": ["x"], "extra": [99]})
        obj = MinimalSchema(df, strict=False)
        assert "extra" in obj.fr_data.columns

    def test_strict_true_rejects_extra_columns(self):
        """strict=True rejects DataFrames with extra columns."""
        df = pd.DataFrame({"col_a": [1], "col_b": ["x"], "extra": [99]})
        with pytest.raises(ValidationError) as exc_info:
            MinimalSchema(df, strict=True)
        # Should mention the extra column
        assert (
            "extra" in str(exc_info.value).lower()
            or "strict" in str(exc_info.value).lower()
        )

    def test_strict_true_accepts_exact_columns(self):
        """strict=True accepts DataFrames with exactly the schema columns."""
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        obj = MinimalSchema(df, strict=True)
        assert len(obj) == 2
        assert list(obj.fr_data.columns) == ["col_a", "col_b"]


# ===========================================================================
# SECTION 20: Schema Inheritance
# ===========================================================================


class TestSchemaInheritance:
    def test_child_inherits_parent_columns(self):
        """Child schema includes parent's columns."""
        assert "id" in ChildSchema._fr_schema
        assert "name" in ChildSchema._fr_schema
        assert "score" in ChildSchema._fr_schema

    def test_child_validates_all_columns(self):
        """Child schema validates both parent and child columns."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "score": [5.0, 10.0]})
        obj = ChildSchema(df)
        assert len(obj) == 2

    def test_child_enforces_parent_constraints(self):
        """Parent-defined constraints still apply on child."""
        df = pd.DataFrame({"id": [1, 1], "name": ["a", "b"], "score": [5.0, 10.0]})
        with pytest.raises(ConstraintViolationError, match="field_uniqueness"):
            ChildSchema(df)

    def test_child_enforces_own_constraints(self):
        """Child-defined constraints also apply."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "score": [-1.0, 10.0]})
        with pytest.raises(ConstraintViolationError, match="greater_than_or_equal_to"):
            ChildSchema(df)


# ===========================================================================
# SECTION 21: Exception Hierarchy
# ===========================================================================


class TestExceptionHierarchy:
    def test_missing_column_is_validation_error(self):
        """MissingColumnError is a subclass of ValidationError."""
        assert issubclass(MissingColumnError, ValidationError)

    def test_type_mismatch_is_validation_error(self):
        """TypeMismatchError is a subclass of ValidationError."""
        assert issubclass(TypeMismatchError, ValidationError)

    def test_constraint_violation_is_validation_error(self):
        """ConstraintViolationError is a subclass of ValidationError."""
        assert issubclass(ConstraintViolationError, ValidationError)

    def test_validation_error_is_frameright_error(self):
        """ValidationError is a subclass of StructFrameError."""
        assert issubclass(ValidationError, StructFrameError)

    def test_schema_error_is_frameright_error(self):
        """SchemaError is a subclass of StructFrameError."""
        assert issubclass(SchemaError, StructFrameError)

    def test_catch_all_with_base(self):
        """All errors can be caught with StructFrameError."""
        df = pd.DataFrame({"wrong": [1]})
        with pytest.raises(StructFrameError):
            MinimalSchema(df)


# Tests for deleted wrapper methods removed - use native DataFrame methods directly


# ===========================================================================
# SECTION 24: Type Coercion Additional Cases
# ===========================================================================


class TestCoerceAdditional:
    class FloatSchema(Schema):
        val: Col[float]

    def test_coerce_strings_to_float(self):
        """Coercion converts string columns to float."""

        df = pd.DataFrame({"val": ["1.5", "2.5", "3.5"]})
        obj = self.FloatSchema(df, coerce=True)
        assert obj.val[0] == 1.5

    def test_coerce_to_bool(self):
        """Coercion converts to bool."""

        class BoolSchema(Schema):
            flag: Col[bool] = Field(nullable=False)

        df = pd.DataFrame({"flag": [1, 0, 1]})
        obj = BoolSchema(df, coerce=True)
        assert obj.flag[0]

    def test_coerce_to_datetime(self):
        """Coercion converts strings to datetime."""

        class DtSchema(Schema):
            ts: Col[datetime]

        df = pd.DataFrame({"ts": ["2024-01-01", "2024-06-15"]})
        obj = DtSchema(df, coerce=True)
        assert pd.api.types.is_datetime64_any_dtype(obj.ts)

    def test_coerce_missing_column_skipped(self):
        """Coercion skips columns not present in dataframe."""
        df = pd.DataFrame({"col_a": ["1", "2"], "col_b": ["a", "b"]})
        # Should not error even though we don't have missing optional cols
        obj = MinimalSchema(df, coerce=True)
        assert len(obj) == 2


# ===========================================================================
# SECTION 25: fr_validate After Mutation
# ===========================================================================


class TestRevalidation:
    def test_revalidate_catches_mutation(self, valid_df):
        """fr_validate catches invalid data after mutation."""
        users = UserData(valid_df, validate=True)
        users.engagement_score = pd.Series([-5.0, 200.0, 50.0])
        with pytest.raises(ConstraintViolationError):
            users.fr_validate()

    def test_revalidate_passes_after_fix(self, valid_df):
        """fr_validate passes after fixing mutated data."""
        users = UserData(valid_df, validate=False)
        result = users.fr_validate()
        assert result is users


# ===========================================================================
# SECTION 27: __contains__ with Alias
# ===========================================================================


class TestContainsAlias:
    def test_contains_uses_df_column_name(self, valid_df):
        """__contains__ checks actual DataFrame column names."""
        users = UserData(valid_df)
        # The alias "SUBSCRIPTION_TIER" is the real column name
        assert "SUBSCRIPTION_TIER" in users

    def test_contains_also_checks_attr_name(self, valid_df):
        """__contains__ also checks Python attribute names, even with aliases."""
        users = UserData(valid_df)
        # "tier" is the attribute name, and it should also work
        assert "tier" in users


# ===========================================================================
# SECTION 28: __repr__ Detail Checks
# ===========================================================================


class TestReprDetails:
    def test_repr_shows_column_count(self, valid_df):
        """__repr__ shows column count."""
        users = UserData(valid_df)
        r = repr(users)
        assert "6 cols" in r

    def test_repr_shows_required_optional(self, valid_df):
        """__repr__ shows required and optional counts."""
        users = UserData(valid_df)
        r = repr(users)
        assert "5 required" in r
        assert "1 optional" in r


# ===========================================================================
# SECTION 29: __eq__ Cross-Type
# ===========================================================================


class TestEqCrossType:
    def test_eq_different_frameright_subclass(self):
        """__eq__ returns NotImplemented for different Schema subclasses."""
        df = pd.DataFrame({"col_a": [1], "col_b": ["x"]})
        a = MinimalSchema(df)

        class OtherSchema(Schema):
            col_a: Col[int]
            col_b: Col[str]

        b = OtherSchema(df)
        assert a.__eq__(b) is NotImplemented


# ===========================================================================
# SECTION 30: fr_schema_info Full Content
# ===========================================================================


class TestSchemaInfoFull:
    def test_schema_info_has_all_keys(self):
        """fr_schema_info dicts include nullable, unique, constraints, description."""
        info = DescribedSchema.fr_schema_info()
        keys = info[0].keys()
        assert "nullable" in keys
        assert "unique" in keys
        assert "constraints" in keys
        assert "description" in keys

    def test_schema_info_description_values(self):
        """fr_schema_info stores description correctly."""
        info = DescribedSchema.fr_schema_info()
        amt_row = next(r for r in info if r["attribute"] == "amount")
        assert amt_row["description"] == "Transaction amount in USD"

    def test_schema_info_constraints_dict(self):
        """fr_schema_info stores constraints as a dict."""
        info = DescribedSchema.fr_schema_info()
        amt_row = next(r for r in info if r["attribute"] == "amount")
        assert isinstance(amt_row["constraints"], dict)
        assert "ge" in amt_row["constraints"]

    def test_schema_info_nullable_and_unique(self):
        """fr_schema_info stores nullable and unique flags."""
        info = StrictSchema.fr_schema_info()
        id_row = next(r for r in info if r["attribute"] == "id")
        assert not id_row["nullable"]
        assert id_row["unique"]


# ===========================================================================
# SECTION 31: fr_from_dict kwargs passthrough
# ===========================================================================


import pytest  # noqa: E402

from frameright import Field  # noqa: E402
from frameright.pandas import Col, Schema  # noqa: E402


def test_integer_coercion_nullable():
    class IntSchema(Schema):
        count: Col[int] = Field(nullable=True)

    # DataFrame with strings that can be coerced, and a None
    df = pd.DataFrame({"count": ["1", "2", None]})

    # This should coerce to Int64 [1, 2, <NA>]
    # Before the fix, errors='coerce' would produce float64 with NaNs [1.0, 2.0, NaN]
    # which is technically allowed as numeric, but Int64 is preferred for Col[int]
    obj = IntSchema(df, coerce=True, coerce_errors="coerce")

    assert str(obj.count.dtype) == "Int64"
    assert obj.count[0] == 1
    assert pd.isna(obj.count[2])


def test_integer_coercion_with_floats_safe():
    class IntSchema(Schema):
        count: Col[int]

    # Integers represented as floats (e.g. from JSON)
    df = pd.DataFrame({"count": [1.0, 2.0]})

    obj = IntSchema(df, coerce=True)
    assert str(obj.count.dtype) == "Int64"
    assert obj.count[0] == 1


def test_integer_coercion_with_floats_lossy():
    class IntSchema(Schema):
        count: Col[int]

    # Real floats
    df = pd.DataFrame({"count": [1.5, 2.0]})

    # coerce -> to_numeric -> [1.5, 2.0] (float)
    # astype("Int64") -> TypeError (cannot safely cast) -> catch -> keep float
    # fr_validate -> checks is_integer_dtype(float) -> False -> Raises TypeMismatchError

    # This ensures we don't silently truncate 1.5 to 1
    with pytest.raises(TypeMismatchError):
        IntSchema(df, coerce=True)


def test_boolean_coercion_strings():
    class BoolSchema(Schema):
        flag: Col[bool]

    df = pd.DataFrame({"flag": ["True", "false", "YES", "no", "1", "0"]})
    obj = BoolSchema(df, coerce=True)

    expected = [True, False, True, False, True, False]
    assert obj.flag.to_list() == expected


def test_nullable_constraint_logic_detailed():
    """Verify fix for the critical bug + other constraints."""

    class ConstraintSchema(Schema):
        val: Col[float] = Field(ge=0, le=10, nullable=True)
        code: Col[str] = Field(min_length=3, nullable=True)

    df = pd.DataFrame({"val": [5.0, None, 1.0], "code": ["ABC", None, "DEF"]})

    # Should pass (NaNs skipped)
    obj = ConstraintSchema(df)
    assert len(obj) == 3

    # Fail case
    df_fail = pd.DataFrame(
        {
            "val": [-1.0, None, 1.0],  # -1.0 violates ge=0
            "code": ["ABC", None, "DEF"],
        }
    )

    from frameright.exceptions import ConstraintViolationError

    with pytest.raises(ConstraintViolationError) as exc:
        ConstraintSchema(df_fail)
    assert "greater_than_or_equal_to" in str(exc.value)


# ---------------------------------------------------------------------------
# Tests for Issue Fixes (non-Col annotations, boolean coercion, __contains__)
# ---------------------------------------------------------------------------


def test_non_col_annotation_raises_schema_error():
    """Schema parsing should reject annotations that are not Col[T] or Optional[Col[T]]."""
    with pytest.raises(SchemaError) as exc:

        class BadSchema(Schema):
            price: float  # Missing Col[] wrapper
            name: Col[str]

    assert "must be annotated as Col[T]" in str(exc.value)
    assert "price" in str(exc.value)


def test_typo_col_annotation_raises_schema_error():
    """Typos like Col[flaot] should still be caught as schema errors."""
    with pytest.raises(SchemaError) as exc:

        class TypoSchema(Schema):
            value: Col[int]
            amount: int  # Typo: missing Col wrapper

    assert "amount" in str(exc.value)
    assert "must be annotated as Col[T]" in str(exc.value)


def test_optional_non_col_raises_schema_error():
    """Optional[float] without Col should also raise SchemaError."""
    with pytest.raises(SchemaError) as exc:

        class BadOptionalSchema(Schema):
            count: Optional[int]  # Missing Col wrapper

    assert "count" in str(exc.value)


def test_boolean_coercion_raises_on_unknown_strings():
    """Boolean coercion should raise error for unrecognized strings when errors='raise'."""

    class BoolSchema(Schema):
        flag: Col[bool]

    df = pd.DataFrame({"flag": ["true", "maybe", "false"]})

    # Test with coerce which actually performs type coercion
    with pytest.raises((ValueError, TypeError)) as exc:
        BoolSchema(df, coerce=True, coerce_errors="raise")

    assert "maybe" in str(exc.value)


def test_boolean_coercion_accepts_valid_strings():
    """Boolean coercion should accept standard true/false strings."""

    class BoolSchema(Schema):
        flag: Col[bool]

    # Test various valid representations
    df = pd.DataFrame(
        {"flag": ["true", "false", "1", "0", "yes", "no", "on", "off", "TRUE", "FALSE"]}
    )

    obj = BoolSchema(df, coerce=True, coerce_errors="raise")
    assert len(obj) == 10
    # First value should be True
    assert obj.flag[0] is True or obj.flag[0]
    # Second value should be False
    assert obj.flag[1] is False or not obj.flag[1]


def test_boolean_coercion_handles_na_values():
    """Boolean coercion should preserve NA/None values."""

    class BoolSchema(Schema):
        flag: Col[bool]

    df = pd.DataFrame({"flag": ["true", None, "false", pd.NA]})

    obj = BoolSchema(df, coerce=True, coerce_errors="raise")
    assert len(obj) == 4
    # Check that NA values are preserved
    assert pd.isna(obj.flag[1])
    assert pd.isna(obj.flag[3])


def test_boolean_coercion_with_errors_coerce():
    """Boolean coercion with errors='coerce' should set unknown values to NA."""

    class BoolSchema(Schema):
        flag: Col[bool]

    df = pd.DataFrame({"flag": ["true", "maybe", "false", "unknown"]})

    # With errors='coerce', unknown values should become NA
    obj = BoolSchema(df, coerce=True, coerce_errors="coerce")
    assert len(obj) == 4
    assert obj.flag[0] is True or obj.flag[0]
    assert pd.isna(obj.flag[1])  # "maybe" -> NA
    assert obj.flag[2] is False or not obj.flag[2]
    assert pd.isna(obj.flag[3])  # "unknown" -> NA


def test_contains_checks_python_attribute_names():
    """__contains__ should check Python attribute names, not just raw column names."""

    class AliasSchema(Schema):
        tier: Col[str] = Field(alias="customer_tier")
        status: Col[str]

    df = pd.DataFrame(
        {"customer_tier": ["Free", "Pro"], "status": ["active", "inactive"]}
    )

    obj = AliasSchema(df, validate=False)

    # Should find by Python attribute name
    assert "tier" in obj
    assert "status" in obj

    # Should also find by raw DataFrame column name
    assert "customer_tier" in obj

    # Should not find non-existent columns
    assert "subscription" not in obj


def test_contains_works_without_aliases():
    """__contains__ should work normally when no aliases are used."""

    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "username": ["alice", "bob", "charlie"],
            "is_active": [True, False, True],
            "engagement_score": [85.5, 42.0, 91.2],
            "SUBSCRIPTION_TIER": ["Free", "Pro", "Enterprise"],
            "lifetime_value": [None, 199.99, None],
        }
    )

    obj = UserData(df, validate=False)

    # All Python attribute names should be checkable
    assert "user_id" in obj
    assert "username" in obj
    assert "is_active" in obj
    assert "engagement_score" in obj
    assert "tier" in obj  # This has an alias
    assert "lifetime_value" in obj

    # Raw column name for alias should also work
    assert "SUBSCRIPTION_TIER" in obj
    # Raw column name for alias should also work
    assert "SUBSCRIPTION_TIER" in obj
    # Raw column name for alias should also work
    assert "SUBSCRIPTION_TIER" in obj
    assert "SUBSCRIPTION_TIER" in obj
    assert "SUBSCRIPTION_TIER" in obj
    assert "SUBSCRIPTION_TIER" in obj
    assert "SUBSCRIPTION_TIER" in obj
    assert "SUBSCRIPTION_TIER" in obj
    assert "SUBSCRIPTION_TIER" in obj
