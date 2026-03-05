import pytest
import pandas as pd
from structframe import StructFrame, Field
from structframe.typing import Col
from typing import Optional


# --- 1. Define a "Kitchen Sink" Schema for Testing ---
class UserData(StructFrame):
    # Basic Dtypes
    user_id: Col[int]
    username: Col[str]
    is_active: Col[bool]

    # Field Constraints & Aliasing
    engagement_score: Col[float] = Field(ge=0.0, le=100.0)
    tier: Col[str] = Field(
        alias="SUBSCRIPTION_TIER", isin=["Free", "Pro", "Enterprise"]
    )

    # Optional Columns
    lifetime_value: Optional[Col[float]] = Field(ge=0.0)


# --- 2. Pytest Fixtures ---
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


# --- 3. The Tests ---


def test_successful_initialization(valid_df):
    """Test the happy path: valid data creates a valid object with correct properties."""
    users = UserData(valid_df)

    assert len(users) == 3
    # Check that aliasing works as a property
    assert users.tier.iloc[0] == "Pro"
    # Check that standard properties work
    assert users.is_active.iloc[1] == False


def test_missing_required_column(valid_df):
    """Ensure missing REQUIRED columns throw a ValueError."""
    bad_df = valid_df.drop(columns=["username"])

    with pytest.raises(ValueError, match="Missing required columns: \\['username'\\]"):
        UserData(bad_df)


def test_missing_optional_column_is_safe(valid_df_missing_optional):
    """Ensure missing OPTIONAL columns do not throw an error and return None."""
    users = UserData(valid_df_missing_optional)

    # The dataframe was accepted safely
    assert len(users) == 3
    # Accessing the missing optional column returns None instead of a KeyError
    assert users.lifetime_value is None


def test_property_setter_updates_dataframe(valid_df):
    """Ensure modifying the attribute updates the underlying hidden _sf_df."""
    users = UserData(valid_df)

    # Update the score using the beautiful syntax
    users.engagement_score = users.engagement_score + 10.0

    # Verify the underlying Pandas dataframe was actually modified
    assert users.sf_data["engagement_score"].iloc[0] == 95.5


def test_runtime_dtype_validation_int(valid_df):
    """Ensure passing strings into an int column throws a TypeError."""
    valid_df["user_id"] = ["1", "2", "3"]  # Change to strings

    with pytest.raises(TypeError, match="must be integer dtype"):
        UserData(valid_df)


def test_runtime_dtype_validation_float(valid_df):
    """Ensure passing strings into a float column throws a TypeError."""
    valid_df["engagement_score"] = ["85.5", "12.0", "99.9"]

    with pytest.raises(TypeError, match="must be float dtype"):
        UserData(valid_df)


def test_field_constraint_ge_le(valid_df):
    """Ensure values outside the ge/le boundaries throw a ValueError."""
    # Break the 'le=100.0' constraint
    valid_df.loc[0, "engagement_score"] = 150.0

    with pytest.raises(ValueError, match="must be <= 100.0"):
        UserData(valid_df)

    # Break the 'ge=0.0' constraint on the optional column
    valid_df.loc[0, "engagement_score"] = 50.0  # Fix previous error
    valid_df.loc[0, "lifetime_value"] = -10.0

    with pytest.raises(ValueError, match="must be >= 0.0"):
        UserData(valid_df)


def test_field_constraint_isin(valid_df):
    """Ensure categorical 'isin' constraints reject invalid values."""
    valid_df.loc[1, "SUBSCRIPTION_TIER"] = (
        "SuperPro"  # Not in ["Free", "Pro", "Enterprise"]
    )

    with pytest.raises(ValueError, match="contains values not in"):
        UserData(valid_df)


def test_sf_filter_returns_new_instance(valid_df):
    """Ensure that filtering returns a new StructFrame without altering the original."""
    users = UserData(valid_df)

    # Slicing the dataframe
    active_users = users.sf_filter(users.is_active)

    # Check that it returned a UserData object, not a raw DataFrame
    assert isinstance(active_users, UserData)

    # Check that the subset has the correct row count
    assert len(active_users) == 2

    # Check that the original object was untouched (copy=False semantics)
    assert len(users) == 3


def test_repr_formatting(valid_df):
    """Ensure the custom __repr__ outputs the correct dimensions."""
    users = UserData(valid_df)
    representation = repr(users)

    assert "<UserData: 3 rows x 6 cols>" in representation
