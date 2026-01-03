import pandas as pd

from aws_cost_tool.service_base import ServiceBase


def extract_usage_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts Usage costs broken down by Region and classifies the type of spending.
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "Amazon Elastic Compute Cloud" service.
    This extraction function preserves the original index so it can be used for
    subtraction with the original DataFrame later
    """
    if df.empty:
        return df

    df = df[df["Cost"] > 0.001]
    df = df[df["Usage_type"].str.contains("Usage", case=False, na=False)].copy()

    # Detect region prefix: 2-3 letters, follows by digit and a hyphen.
    region_pattern = r"^[a-zA-Z]{2}(?:[a-zA-Z]+)?\d-"
    df["Usage_type"] = df["Usage_type"].str.replace(region_pattern, "", regex=True)
    df["Subtype"] = df["Usage_type"].str.split(":", n=1).str[0]
    return df


def extract_data_transfer_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts data transfer costs broken down by Region and transfer type.
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "Amazon Elastic Compute Cloud" service.
    This extraction function preserves the original index so it can be used for
    subtraction with the original DataFrame later
    """
    if df.empty:
        return df

    # Filter for Data Transfer, CloudFront usage types, all ends with "Bytes".
    df = df[df["Cost"] > 0.001]
    df = df[df["Usage_type"].str.contains("Byte", case=False, na=False)].copy()
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
        return "Amazon Elastic Compute Cloud"

    @property
    def shortname(self) -> str:
        """The display name of the service"""
        return "EC2"

    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        return self.categorize_usage_costs(df, extractors=EC2_EXTRACTOR)
