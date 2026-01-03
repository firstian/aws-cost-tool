import pandas as pd
import pytest

from aws_cost_tool.services.ec2 import extract_data_transfer_costs, extract_usage_costs


@pytest.fixture
def sample_usage_data():
    """Provides a mix of EC2 usage and data transfer types."""
    data = {
        "Usage_type": [
            "USW2-SpotUsage:m7i-flex.large",  # Standard Region + Usage + Instance
            "us-east-1-BoxUsage:t3.medium",  # Lowercase Region + Usage
            "BoxUsage:t2.micro",  # No Region Prefix
            "USW2-DataTransfer-Out-Bytes",  # Data Transfer
            "USE1-NodeUsage:db.r5.large",  # Another Usage type
            "USW2-SpotUsage:m7i.small",  # Low cost (should be filtered)
            "NoUsageHere",  # Should be filtered by string match
        ],
        "Cost": [10.0, 5.0, 2.0, 1.5, 3.0, 0.0001, 10.0],
    }
    return pd.DataFrame(data)


def test_extract_usage_costs(sample_usage_data):
    # Execute
    result = extract_usage_costs(sample_usage_data)

    # Assertions
    # 1. Check low cost (0.0001) was removed
    assert 0.0001 not in result["Cost"].values

    # 2. Check that only "Usage" types remain
    assert all("Usage" in x for x in result["Subtype"])

    # 3. Check region stripping and colon splitting
    # USW2-SpotUsage:m7i-flex.large -> SpotUsage
    assert "SpotUsage" in result["Subtype"].values
    # us-east-1-BoxUsage:t3.medium -> BoxUsage
    assert "BoxUsage" in result["Subtype"].values

    # 4. Check that DataTransfer was filtered out
    assert "Data Transfer" not in result["Subtype"].values


def test_extract_data_transfer_costs(sample_usage_data):
    # Execute
    result = extract_data_transfer_costs(sample_usage_data)

    # Assertions
    # 1. Should only find rows containing "Byte"
    assert len(result) == 1
    assert result.iloc[0]["Subtype"] == "Data Transfer"
    assert "DataTransfer-Out-Bytes" in result.iloc[0]["Usage_type"]


def test_empty_dataframe():
    df_empty = pd.DataFrame(columns=["Usage_type", "Cost"])

    result_usage = extract_usage_costs(df_empty)
    result_dt = extract_data_transfer_costs(df_empty)

    assert result_usage.empty
    assert result_dt.empty


def test_usage_no_colon():
    """Test behavior when 'Usage' is present but no colon exists."""
    data = {"Usage_type": ["USW2-SomeUsage"], "Cost": [1.0]}
    df = pd.DataFrame(data)

    result = extract_usage_costs(df)
    # Should strip region and return the rest since no colon exists
    assert result.iloc[0]["Subtype"] == "SomeUsage"
