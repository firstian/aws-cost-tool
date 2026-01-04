import pandas as pd

from aws_cost_tool.service_base import ServiceBase


def extract_standard_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts standard access costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Elastic File System". This extraction function preserves the
    original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("IA", na=False)
        df = df[~mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Standard"
    return df


def extract_ia_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts infrequent access costs from DataFrame from fetch_service_costs_by_usage
    for "Amazon Elastic File System". This extraction function preserves
    the original index so it can be used to subtract from the original DataFrame
    later.
    """
    if not df.empty:
        mask = df["Usage_type"].str.contains("IA", na=False)
        df = df[mask].copy()
        ServiceBase.strip_region_prefix_from_usage(df)
        df["Subtype"] = "Infrequent"
    return df


EFS_EXTRACTOR = {
    "Standard": extract_standard_costs,
    "Infrequent": extract_ia_costs,
}


class EFS(ServiceBase):
    @property
    def name(self) -> str:
        """The display name of the service used by Cost Explorer."""
        return "Amazon Elastic File System"

    @property
    def shortname(self) -> str:
        """The display name of the service"""
        return "EFS"

    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        return self.categorize_usage_costs(df, extractors=EFS_EXTRACTOR)
