import pandas as pd
import pytest

# Assuming the file is named s3_processor.py
from aws_cost_tool.services.s3 import (
    extract_data_transfer_costs,
    extract_request_costs,
    extract_storage_costs,
)


@pytest.fixture
def s3_sample_data():
    """Fixture containing standard S3 billing line items."""
    return pd.DataFrame(
        {
            "Usage_type": [
                "USW2-TimedStorage-ByteHrs",  # Standard Storage
                "USE1-Standard-IA-TimedStorage-ByteHrs",  # IA Storage
                "USE1-Requests-Tier1",  # PUT/LIST Requests
                "USW2-Requests-Tier2",  # GET/SELECT Requests
                "DataTransfer-Out-Bytes",  # Data Transfer
                "USW2-CloudFront-Out-Bytes",  # Data Transfer (CF)
                "USE1-EarlyDeletion-ByteHrs",  # doesn't match filters, ignored
            ],
            "Cost": [20.0, 5.0, 2.5, 1.2, 0.8, 0.4, 0.1],
        }
    )


def test_extract_s3_storage_costs(s3_sample_data):
    result = extract_storage_costs(s3_sample_data)

    # Should catch both Standard and Standard-IA storage
    assert len(result) == 2
    assert all(result["Subtype"] == "Storage")

    # Verify region stripping worked via ServiceBase
    assert "TimedStorage-ByteHrs" in result["Usage_type"].values
    assert "Standard-IA-TimedStorage-ByteHrs" in result["Usage_type"].values


def test_extract_s3_request_costs(s3_sample_data):
    result = extract_request_costs(s3_sample_data)

    # Should catch Tier1 and Tier2 requests
    assert len(result) == 2
    assert all(result["Subtype"] == "Request")

    # Verify usage type cleaning
    assert "Requests-Tier1" in result["Usage_type"].values
    assert "Requests-Tier2" in result["Usage_type"].values


def test_extract_s3_data_transfer_costs(s3_sample_data):
    result = extract_data_transfer_costs(s3_sample_data)

    # Should catch anything ending in Bytes
    assert len(result) == 2
    assert all(result["Subtype"] == "Data Transfer")

    # Per your code, Data Transfer doesn't call strip_region_prefix_from_usage
    # So we check for the original string
    assert "USW2-CloudFront-Out-Bytes" in result["Usage_type"].values


def test_s3_index_preservation(s3_sample_data):
    """Verify indices are preserved for multi-index categorization."""
    storage_res = extract_storage_costs(s3_sample_data)
    request_res = extract_request_costs(s3_sample_data)

    # Row 0 is Storage
    assert 0 in storage_res.index
    # Row 2 is Request
    assert 2 in request_res.index


def test_s3_empty_dataframe():
    df_empty = pd.DataFrame(columns=["Usage_type", "Cost"])
    assert extract_storage_costs(df_empty).empty
    assert extract_request_costs(df_empty).empty
