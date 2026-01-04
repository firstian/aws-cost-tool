import pandas as pd

from aws_cost_tool.service_base import ServiceBase


def extract_storage_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts storage costs from DataFrame from fetch_service_costs_by_usage for
    "Amazon Simple Storage Service". This extraction function preserves the
    original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("TimedStorage", case=False, na=False)
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Storage"
    return df


def extract_request_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts request costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Simple Storage Service". This extraction function preserves the
    original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("Requests-Tier", case=False, na=False)
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Request"
    return df


def extract_data_transfer_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts data transfer costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Simple Storage Service". This extraction function preserves the
    original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.endswith("Bytes", na=False)
        df = df[mask].copy()
        df["Subtype"] = "Data Transfer"
    return df


S3_EXTRACTOR = {
    "Storage": extract_storage_costs,
    "Request": extract_request_costs,
    "Data Transfer": extract_data_transfer_costs,
}


class S3(ServiceBase):
    @property
    def name(self) -> str:
        """The display name of the service used by Cost Explorer."""
        return "Amazon Simple Storage Service"

    @property
    def shortname(self) -> str:
        """The display name of the service"""
        return "S3"

    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        return self.categorize_usage_costs(df, extractors=S3_EXTRACTOR)
