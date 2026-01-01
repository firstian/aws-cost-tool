import pandas as pd
import pytest

from aws_cost_tool.services.ec2_other import (
    extract_data_transfer_costs,
    extract_ebs_costs,
    extract_nat_gateway_costs,
)


@pytest.fixture
def ec2_other_df():
    """A sample DataFrame representing AWS 'EC2 - Other' usage data."""
    return pd.DataFrame(
        {
            "Usage_type": [
                "USE1-EBS:VolumeUsage.gp3",  # EBS Volume
                "USW2-EBS:SnapshotUsage",  # EBS Snapshot
                "EU-EBS:VolumeP-Throughput",  # EBS Throughput (IOPS)
                "USE1-EBSOptimazed:i3.large",  # Should be EXCLUDED
                "USE1-NatGateway-Hours",  # NAT Hours
                "USW2-NatGateway-Bytes",  # NAT Bytes
                "USE1-DataTransfer-Out-Bytes",  # Data Transfer
                "VpcPeering-In-Bytes",  # VPC Peering
                "USE1-EBS:VolumeUsage.gp2",  # Low cost - Should be EXCLUDED
            ],
            "Cost": [10.0, 5.0, 2.0, 1.0, 8.0, 15.0, 4.0, 3.0, 0.0001],
        },
        index=[101, 102, 103, 104, 105, 106, 107, 108, 109],
    )


## --- Tests for EBS Extraction ---


def test_extract_ebs_costs(ec2_other_df):
    result = extract_ebs_costs(ec2_other_df)

    # 1. Check Filtering
    # Excludes: EBSOptimazed (104), Low Cost (109), NAT (105, 106), DT (107, 108)
    assert len(result) == 3
    assert 104 not in result.index
    assert 109 not in result.index

    # 2. Check Regex Extraction (Removes region prefix)
    assert result.loc[101, "Usage_type"] == "EBS:VolumeUsage.gp3"

    # 3. Check Subtype classification
    assert result.loc[101, "Subtype"] == "EBS Volume"
    assert result.loc[102, "Subtype"] == "EBS Snapshot"
    assert result.loc[103, "Subtype"] == "EBS Throughput"


def test_extract_ebs_empty():
    assert extract_ebs_costs(pd.DataFrame()).empty


## --- Tests for NAT Gateway Extraction ---


def test_extract_nat_gateway_costs(ec2_other_df):
    result = extract_nat_gateway_costs(ec2_other_df)

    # 1. Check Filtering
    assert len(result) == 2
    assert set(result.index) == {105, 106}

    # 2. Check Regex
    assert result.loc[105, "Usage_type"] == "NatGateway-Hours"

    # 3. Check Subtype
    assert result.loc[105, "Subtype"] == "NAT Gateway Hours"
    assert result.loc[106, "Subtype"] == "NAT Gateway Bytes"


def test_extract_nat_gateway_empty():
    assert extract_nat_gateway_costs(pd.DataFrame()).empty


## --- Tests for Data Transfer Extraction ---


def test_extract_data_transfer_costs(ec2_other_df):
    result = extract_data_transfer_costs(ec2_other_df)

    # 1. Check Filtering (Should include both DataTransfer and VpcPeering)
    assert len(result) == 2
    assert set(result.index) == {107, 108}

    # 2. Check Subtype
    assert all(result["Subtype"] == "Data Transfer")

    # 3. Check Regex
    assert result.loc[107, "Usage_type"] == "DataTransfer-Out-Bytes"
    assert result.loc[108, "Usage_type"] == "VpcPeering-In-Bytes"


def test_extract_data_transfer_empty():
    assert extract_data_transfer_costs(pd.DataFrame()).empty


## --- Test Edge Cases ---


def test_extractors_handle_no_matches():
    df = pd.DataFrame({"Usage_type": ["Random-Usage"], "Cost": [10.0]})
    assert extract_ebs_costs(df).empty
    assert extract_nat_gateway_costs(df).empty
    assert extract_data_transfer_costs(df).empty
