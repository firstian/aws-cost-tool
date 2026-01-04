import numpy as np
import pandas as pd

from aws_cost_tool.service_base import ServiceBase


def extract_ebs_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts EBS costs broken down by Region and classifies the type of spending.
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "EC2 - Other" service.
    This extraction function preserves the original index so it can be used for
    subtraction with the original DataFrame later
    """
    if df.empty:
        return df

    # Filter for EBS rows and remove region prefix from the type label.
    df = df[df["Cost"] > 0.001]
    mask = df.Usage_type.str.contains("EBS") & ~df.Usage_type.str.contains(
        "EBSOptimized"
    )
    ebs_df = df[mask].copy()
    ebs_df["Usage_type"] = ebs_df["Usage_type"].str.extract(r"(EBS:.*)")

    # Add a column to categorize the rows.
    conditions = [
        ebs_df["Usage_type"].str.contains("VolumeUsage", case=False),
        ebs_df["Usage_type"].str.contains("SnapshotUsage", case=False),
        ebs_df["Usage_type"].str.contains("Throughput|IOPS", case=False),
    ]

    choices = ["EBS Volume", "EBS Snapshot", "EBS Throughput"]
    ebs_df["Subtype"] = np.select(conditions, choices, default="Other")
    return ebs_df


def extract_nat_gateway_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts NAT Gateway costs broken down by Region and cost type (hours vs data
    processed).
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "EC2 - Other" service.
    This extraction function preserves the original index so it can be used for
    subtraction with the original DataFrame later
    """
    if df.empty:
        return df

    # Filter for NAT Gateway usage types
    df = df[df["Cost"] > 0.001]
    mask = df["Usage_type"].str.contains("NatGateway", case=False, na=False)
    nat_df = df[mask].copy()
    nat_df["Usage_type"] = nat_df["Usage_type"].str.extract(r"(NatGateway.*)")

    # Add a column to categorize the rows.
    conditions = [
        nat_df["Usage_type"].str.contains("Hours", case=False),
        nat_df["Usage_type"].str.contains("Bytes", case=False),
    ]

    choices = ["NAT Gateway Hours", "NAT Gateway Bytes"]
    nat_df["Subtype"] = np.select(conditions, choices, default="Other")
    return nat_df


def extract_data_transfer_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts data transfer costs broken down by Region and transfer type.
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "EC2 - Other" service.
    This extraction function preserves the original index so it can be used for
    subtraction with the original DataFrame later
    """
    if df.empty:
        return df

    # Filter for Data Transfer and VPC usage types
    df = df[df["Cost"] > 0.001]
    mask = df["Usage_type"].str.contains("DataTransfer", case=False, na=False)
    dt_df = df[mask].copy()
    dt_df["Usage_type"] = dt_df["Usage_type"].str.extract(r"(DataTransfer.*)")
    mask = df["Usage_type"].str.contains("VpcPeering", case=False, na=False)
    vpc_df = df[mask].copy()
    vpc_df["Usage_type"] = vpc_df["Usage_type"].str.extract(r"(VpcPeering.*)")
    full_dt_df = pd.concat([dt_df, vpc_df])
    full_dt_df["Subtype"] = "Data Transfer"
    return full_dt_df


EC2_OTHER_EXTRACTOR = {
    "EBS": extract_ebs_costs,
    "VPC": extract_nat_gateway_costs,
    "Data Transfer": extract_data_transfer_costs,
}


class EC2Other(ServiceBase):
    @property
    def name(self) -> str:
        """The display name of the service used by Cost Explorer."""
        return "EC2 - Other"

    @property
    def shortname(self) -> str:
        """The display name of the service"""
        return "EC2 Other"

    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        return self.categorize_usage_costs(df, extractors=EC2_OTHER_EXTRACTOR)
