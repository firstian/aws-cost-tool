import pandas as pd

from aws_cost_tool.service_base import ServiceBase


def extract_backup_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts backup costs from DataFrame from fetch_service_costs_by_usage for
    "Amazon Relational Database Service". This extraction function preserves the
    original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("BackupUsage", case=False, na=False)
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Backup"
    return df


def extract_storage_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts storage costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Relational Database Service". This extraction function preserves
    the original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("Storage", case=False, na=False)
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Storage"
    return df


def extract_compute_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts compute costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Relational Database Service". This extraction function preserves
    the original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains(
            r"InstanceUsage|Serverless", case=False, na=False
        )
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Compute"
    return df


def extract_data_transfer_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts data transfer costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Relational Database Service". This extraction function preserves
    the original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.endswith("Bytes", na=False)
        df = df[mask].copy()
        df["Subtype"] = "Data Transfer"
    return df


RDS_EXTRACTOR = {
    "Backup": extract_backup_costs,
    "Storage": extract_storage_costs,
    "Compute": extract_compute_costs,
    "Data Transfer": extract_data_transfer_costs,
}


class RDS(ServiceBase):
    @property
    def name(self) -> str:
        """The display name of the service used by Cost Explorer."""
        return "Amazon Relational Database Service"

    @property
    def shortname(self) -> str:
        """The display name of the service"""
        return "RDS"

    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        return self.categorize_usage_costs(df, extractors=RDS_EXTRACTOR)
