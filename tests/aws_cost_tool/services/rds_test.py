import pandas as pd
import pytest

# Assuming the file is named rds_processor.py
from aws_cost_tool.services.rds import (
    extract_backup_costs,
    extract_compute_costs,
    extract_data_transfer_costs,
    extract_storage_costs,
)


@pytest.fixture
def rds_sample_data():
    """Fixture with a realistic mix of RDS billing line items."""
    return pd.DataFrame(
        {
            "Usage_type": [
                "USW2-InstanceUsage:db.m5.large",  # Compute
                "USE1-ServerlessUsage",  # Compute (Serverless)
                "USW2-StorageUsage",  # Storage
                "USE1-BackupUsage",  # Backup
                "USW2-DataTransfer-Out-Bytes",  # Data Transfer
                "RDS:GP3-Storage",  # Storage (another variant)
                "USE1-VpcEndpoint-Bytes",  # Data Transfer
                "LowCostRow",  # Should be ignored by logic
            ],
            "Cost": [50.0, 10.0, 5.0, 2.0, 1.0, 4.0, 0.5, 0.00001],
        }
    )


def test_extract_compute_costs(rds_sample_data):
    result = extract_compute_costs(rds_sample_data)

    # Should catch both InstanceUsage and Serverless
    assert len(result) == 2
    assert all(result["Subtype"] == "Compute")
    # Verify region stripping
    assert "InstanceUsage:db.m5.large" in result["Usage_type"].values
    assert "ServerlessUsage" in result["Usage_type"].values


def test_extract_storage_costs(rds_sample_data):
    result = extract_storage_costs(rds_sample_data)

    # Should catch StorageUsage and GP3-Storage
    # Note: BackupUsage ALSO contains the word "Storage" in some AWS exports.
    # Your current function logic will catch BackupUsage if it's there.
    storage_types = result["Usage_type"].values
    assert "StorageUsage" in storage_types
    assert "RDS:GP3-Storage" in storage_types
    assert all(result["Subtype"] == "Storage")


def test_extract_backup_costs(rds_sample_data):
    result = extract_backup_costs(rds_sample_data)

    assert len(result) == 1
    assert "BackupUsage" in result["Usage_type"].values
    assert result.iloc[0]["Subtype"] == "Backup"


def test_extract_data_transfer_costs(rds_sample_data):
    result = extract_data_transfer_costs(rds_sample_data)

    # Should catch anything ending in 'Bytes'
    assert len(result) == 2
    assert all(result["Subtype"] == "Data Transfer")
    # Verify region stripping is NOT called here per your code
    assert "USW2-DataTransfer-Out-Bytes" in result["Usage_type"].values


def test_index_alignment(rds_sample_data):
    """Ensure we can still align back to the original data."""
    compute = extract_compute_costs(rds_sample_data)
    backup = extract_backup_costs(rds_sample_data)

    # The first row (index 0) is Compute
    assert 0 in compute.index
    # The fourth row (index 3) is Backup
    assert 3 in backup.index
