import pandas as pd

from aws_cost_tool.service_base import ServiceBase


def extract_usage_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts Usage costs from DataFrame from fetch_service_costs_by_usage for
    "Amazon Elastic Compute Cloud" service. This extraction function preserves
    the original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("Usage", case=False, na=False)
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = df["Usage_type"].str.split(":", n=1).str[0]
    return df


def extract_data_transfer_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts data transfer costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Elastic Compute Cloud" service. This extraction function preserves
    the original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.endswith("Bytes", na=False)
        df = df[mask].copy()
        df["Subtype"] = "Data Transfer"
    return df


EC2_EXTRACTOR = {
    "Usage": extract_usage_costs,
    "Data Transfer": extract_data_transfer_costs,
}


class EC2(ServiceBase):
    @property
    def name(self) -> str:
        """The display name of the service used by Cost Explorer."""
        return "Amazon Elastic Compute Cloud - Compute"

    @property
    def shortname(self) -> str:
        """The display name of the service"""
        return "EC2"

    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        return self.categorize_usage_costs(df, extractors=EC2_EXTRACTOR)
