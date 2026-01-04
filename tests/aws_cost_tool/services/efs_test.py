import pandas as pd
import pytest

# Assuming the code is in a file named efs_processor.py
from aws_cost_tool.services.efs import extract_ia_costs, extract_standard_costs


@pytest.fixture
def sample_efs_data():
    """Sample EFS usage data with various tiers and regions."""
    return pd.DataFrame(
        {
            "Usage_type": [
                "USW2-TimedStorage-ByteHrs",  # Standard
                "USE1-IATimedStorage-ByteHrs",  # IA
                "USW2-ETDataAccess-Bytes",  # Standard
                "USE1-IADataAccess-Bytes",  # IA
                "TimedStorage-ByteHrs",  # Standard (no prefix)
                "IATimedStorage-Z-ByteHrs",  # IA (no prefix)
            ],
            "Cost": [10.0, 1.0, 5.0, 0.5, 8.0, 0.2],
        }
    )


def test_extract_standard_costs(sample_efs_data):
    # Execute
    result = extract_standard_costs(sample_efs_data)

    # Assertions
    # 1. Should have exactly 3 rows (non-IA rows)
    assert len(result) == 3

    # 2. All Subtypes must be 'Standard'
    assert all(result["Subtype"] == "Standard")

    # 3. None of the remaining rows should contain "IA"
    assert not result["Usage_type"].str.contains("IA").any()

    # 4. Verify region stripping (USW2- should be gone)
    # Note: This assumes ServiceBase.strip_region_prefix_from_usage is working
    assert "TimedStorage-ByteHrs" in result["Usage_type"].values
    assert "ETDataAccess-Bytes" in result["Usage_type"].values


def test_extract_ia_costs(sample_efs_data):
    # Execute
    result = extract_ia_costs(sample_efs_data)

    # Assertions
    # 1. Should have exactly 3 rows (IA rows)
    assert len(result) == 3

    # 2. All Subtypes must be 'Infrequent'
    assert all(result["Subtype"] == "Infrequent")

    # 3. All remaining rows MUST contain "IA"
    assert result["Usage_type"].str.contains("IA").all()

    # 4. Verify region stripping
    # USE1-IATimedStorage-ByteHrs -> IATimedStorage-ByteHrs
    assert "IATimedStorage-ByteHrs" in result["Usage_type"].values


def test_extractors_empty_df():
    """Test both extractors with an empty DataFrame."""
    df_empty = pd.DataFrame(columns=["Usage_type", "Cost"])

    res_std = extract_standard_costs(df_empty)
    res_ia = extract_ia_costs(df_empty)

    assert res_std.empty
    assert res_ia.empty


def test_index_preservation(sample_efs_data):
    """
    Crucial test: Ensure the original indices are kept for
    later subtraction/merging logic.
    """
    result_ia = extract_ia_costs(sample_efs_data)

    # The second row in sample_efs_data is an IA row (index 1)
    assert 1 in result_ia.index
    # The fourth row is IA (index 3)
    assert 3 in result_ia.index
