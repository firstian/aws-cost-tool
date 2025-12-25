import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

API_SLEEP_VAL = 0.2


@dataclass
class DateRange:
    """
    A convenient wrapper for date range that works with Cost Explorer. The end
    date is exclusive, following the AWS boto3 convention.
    """

    start: date
    end: date

    def __init__(self, *, start: date | str, end: date | str = "", delta: int = 1):
        self.start = self._to_date(start)
        if end != "":
            self.end = self._to_date(end)
        else:
            if delta <= 0:
                delta = 1
            self.end = self.start + timedelta(days=delta)

        if self.start >= self.end:
            raise ValueError("start date must be < end date")

    def to_time_period(self) -> dict[str, str]:
        return {
            "Start": self.start.isoformat(),
            "End": self.end.isoformat(),
        }

    @staticmethod
    def _to_date(value: date | str) -> date:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except ValueError as e:
                raise ValueError(f"Invalid date string: {value}") from e
        raise TypeError(f"Expected date or str, got {type(value).__name__}")


## Utilities to discover usable tag key and values
def get_tag_keys(ce_client, *, dates: DateRange) -> list[str]:
    """Get the list of all tag keys."""
    try:
        response = ce_client.get_tags(TimePeriod=dates.to_time_period())
        return response.get("Tags", [])
    except Exception as e:
        print(f"Error fetching tag keys: {e}")

    return []


def get_tags_for_key(ce_client, *, tag_key: str, dates: DateRange) -> list[str]:
    """Get the list of all tag values for a given tag key."""
    tag_keys = set()

    try:
        response = ce_client.get_tags(TimePeriod=dates.to_time_period(), TagKey=tag_key)
        tag_keys.update(response.get("Tags", []))
    except Exception as e:
        print(f"Error fetching tag keys: {e}")

    # Filter out all the aws tag values.
    return sorted([key for key in tag_keys if not key.startswith("aws:")])


## Utilities to extract service names
def get_all_aws_services(ce_client, dates: DateRange) -> list[str]:
    """Return the list of services from AWS"""
    response = ce_client.get_dimension_values(
        TimePeriod=dates.to_time_period(),
        Dimension="SERVICE",
        Context="COST_AND_USAGE",
    )

    return sorted([item["Value"] for item in response["DimensionValues"]])


## Functions to retrieve cost data from AWS Cost Explorer with no additional processing.
def fetch_cost_by_region(
    ce_client,
    *,
    dates: DateRange,
    filter_expr: dict[str, Any] | None = None,
    group_by: str = "SERVICE",
    label: str = "",
    granularity: str = "MONTHLY",
) -> pd.DataFrame:
    """
    Fetches cost data and broken down by Region and the specified group_by dimenion,
    with start and end date for each entry. This is the main worker function used
    to implement all the rest of the higher level data fetcher.

    Returns a DataFrame has the following columns:
    StartDate, EndDate, Label, <group_by>, Region, Cost

    where <group_by> is the capitalized version of the argument.
    """
    results = []
    next_page_token = None
    group_by_param = [
        {"Type": "DIMENSION", "Key": group_by.upper()},
        {"Type": "DIMENSION", "Key": "REGION"},
    ]
    time_period = dates.to_time_period()
    # Keys for the API are all caps. For display we want to follow the normal
    # capitalization convention of the boto3 API.
    col_name = group_by.capitalize()

    while True:
        # Build request parameters to avoid next_page_token=None problem with
        # get_cost_and_usage. It doesn't like to be passed explicitly.
        params = {
            "TimePeriod": time_period,
            "Granularity": granularity,
            "Metrics": ["UnblendedCost"],
            "GroupBy": group_by_param,
        }

        if filter_expr:
            params["Filter"] = filter_expr

        # Add the token from a previous call if it exists.
        if next_page_token:
            params["NextPageToken"] = next_page_token

        response = ce_client.get_cost_and_usage(**params)

        for period in response["ResultsByTime"]:
            start = period["TimePeriod"]["Start"]
            end = period["TimePeriod"]["End"]
            for group in period["Groups"]:
                results.append(
                    {
                        "StartDate": start,
                        "EndDate": end,
                        "Label": label,
                        col_name: group["Keys"][0],
                        "Region": group["Keys"][1],
                        "Cost": float(group["Metrics"]["UnblendedCost"]["Amount"]),
                    }
                )
        # Check if there is more data to fetch
        next_page_token = response.get("NextPageToken")
        if not next_page_token:
            break

    df = pd.DataFrame(results)
    if df.empty:
        return pd.DataFrame(
            columns=["StartDate", "EndDate", "Label", col_name, "Region", "Cost"]
        )

    df["StartDate"] = pd.to_datetime(df["StartDate"]).dt.date
    df["EndDate"] = pd.to_datetime(df["EndDate"]).dt.date
    return df


def fetch_service_costs(
    ce_client,
    *,
    dates: DateRange,
    tag_key: str = "",
    tag_values: list[str] | None = None,
    granularity: str = "MONTHLY",
) -> pd.DataFrame:
    """
    Fetches the costs by service for the list of tags, where each tag will be
    assigned a separate label instead of aggregated.
    - If there are no tag_values passed, then it implicitly fetches all the tag
      values under a key.
    - If no tag_key is passed, then everything will be aggregated, ignoring any
      tags.
    """
    df_list = []
    if tag_key:
        if not tag_values:
            tag_values = get_tags_for_key(ce_client, tag_key=tag_key, dates=dates)

        for tag in tag_values:
            df = fetch_cost_by_region(
                ce_client,
                dates=dates,
                filter_expr={"Tags": {"Key": tag_key, "Values": [tag]}},
                label=tag,
                granularity=granularity,
            )
            df_list.append(df)
            # Rate limit safety: AWS CE API is typically limited to ~1-10 requests/sec
            time.sleep(API_SLEEP_VAL)
    else:
        df_list = [
            fetch_cost_by_region(ce_client, dates=dates, granularity=granularity)
        ]

    if not df_list:
        return pd.DataFrame(
            columns=["StartDate", "EndDate", "Label", "Service", "Region", "Cost"]
        )

    return pd.concat(df_list, ignore_index=True)


def fetch_service_costs_by_usage(
    ce_client,
    *,
    service: str,
    dates: DateRange,
    tag_key: str = "",
    tag_values: list[str] | None = None,
    granularity: str = "MONTHLY",
) -> pd.DataFrame:
    """
    Fetches all EC2-Other costs broken down by Region and Usage Type.
    - If there are no tag_values passed, then it implicitly fetches all the tag
      values under a key.
    - If no tag_key is passed, then everything will be aggregated, ignoring any
      tags.
    Returns a dataFrame with columns:
    StartDate, EndDate, Service, Region, Usage_type, Cost
    """
    # Base filter for the selected service.
    base_filter = {"Dimensions": {"Key": "SERVICE", "Values": [service]}}
    group_by = "USAGE_TYPE"

    df_list = []
    if tag_key:
        # Tags specificed, iterate through all the values and label.
        if not tag_values:
            tag_values = get_tags_for_key(ce_client, tag_key=tag_key, dates=dates)

        for tag in tag_values:
            tag_filter = {"Tags": {"Key": tag_key, "Values": [tag]}}
            df = fetch_cost_by_region(
                ce_client,
                dates=dates,
                filter_expr={"And": [base_filter, tag_filter]},
                group_by=group_by,
                label=tag,
                granularity=granularity,
            )
            df_list.append(df)
            # Rate limit safety: AWS CE API is typically limited to ~1-10 requests/sec
            time.sleep(API_SLEEP_VAL)
    else:
        # No tags specified, just get them all.
        df_list = [
            fetch_cost_by_region(
                ce_client,
                dates=dates,
                filter_expr=base_filter,
                group_by=group_by,
                granularity=granularity,
            )
        ]
    if not df_list:
        return pd.DataFrame(
            columns=[
                "StartDate",
                "EndDate",
                "Label",
                "Service",
                group_by.capitalize(),
                "Region",
                "Cost",
            ]
        )

    final_df = pd.concat(df_list, ignore_index=True)
    final_df.insert(3, "Service", service)
    return final_df


## Functions to analyze the retrieved data.
def extract_ebs_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts EBS costs broken down by Region and classifies the type of spending.
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "EC2 - Other" service.
    """
    if df.empty:
        return df

    # Filter for EBS rows and remove region prefix from the type label.
    df = df[df["Cost"] > 0.001]
    mask = df.Usage_type.str.contains("EBS") & ~df.Usage_type.str.contains(
        "EBSOptimazed"
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
    ebs_df["Category"] = np.select(conditions, choices, default="Other")
    return ebs_df


def extract_nat_gateway_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetches NAT Gateway costs broken down by Region and cost type (hours vs data
    processed).
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "EC2 - Other" service.
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
    nat_df["Category"] = np.select(conditions, choices, default="Other")
    return nat_df


def extract_data_transfer_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetches data transfer costs broken down by Region and transfer type.
    It assuming the input is a DataFrame returned by fetch_service_costs_by_usage
    for "EC2 - Other" service.
    """
    if df.empty:
        return df

    # Filter for NAT Gateway usage types
    df = df[df["Cost"] > 0.001]
    mask = df["Usage_type"].str.contains("DataTransfer", case=False, na=False)
    dt_df = df[mask].copy()
    dt_df["Usage_type"] = dt_df["Usage_type"].str.extract(r"(DataTransfer.*)")
    mask = df["Usage_type"].str.contains("VpcPeering", case=False, na=False)
    vpc_df = df[mask].copy()
    vpc_df["Usage_type"] = vpc_df["Usage_type"].str.extract(r"(VpcPeering.*)")

    return pd.concat([dt_df, vpc_df], ignore_index=True)


## Utilites to transform the raw data into more useful summaries.
def summarize_by_columns(
    df: pd.DataFrame, columns: list[str], threshold: float | None = 0.001
):
    """Utility to aggregate costs based on a list of columns."""
    summary_df = df.groupby(columns, as_index=False)["Cost"].sum()
    if threshold is not None and threshold > 0.0:
        summary_df = summary_df[summary_df["Cost"] >= threshold]
    return summary_df


def pivot_data(
    df: pd.DataFrame, *, row_label: str, col_label: str, threshold: float = 0.001
):
    """
    Returns a summary of services cost for each unit of time, pivoted based on
    the specified row and column. This is primarily useful for human consumption.
    """
    summary = summarize_by_columns(df, [row_label, col_label], threshold)
    return summary.pivot_table(
        index=row_label, columns=col_label, values="Cost", aggfunc="sum"
    ).fillna(0.0)


def summarize_ec2_other_by_category(
    ce_client,
    *,
    dates: DateRange,
    tag_key: str = "",
    tag_values: list[str] | None = None,
    granularity: str = "MONTHLY",
) -> pd.DataFrame:
    """
    Provides a high-level summary of EC2-Other costs categorized by resource type.
    Returns a DataFrame with costs summarized by category and region
    """
    df = fetch_service_costs_by_usage(
        ce_client,
        service="EC2 - Other",
        dates=dates,
        tag_key=tag_key,
        tag_values=tag_values,
        granularity=granularity,
    )

    # Fetch individual categories
    df_list = []
    ebs_df = extract_ebs_costs(df)
    if not ebs_df.empty:
        df_list.append(summarize_by_columns(ebs_df, ["StartDate", "Category"]))

    nat_df = extract_nat_gateway_costs(df)
    if not nat_df.empty:
        df_list.append(summarize_by_columns(nat_df, ["StartDate", "Category"]))

    dt_df = extract_data_transfer_costs(df)
    if not dt_df.empty:
        dt_df = dt_df.rename(columns={"Usage_type": "Category"})
        df_list.append(summarize_by_columns(dt_df, ["StartDate", "Category"]))

    all_costs = pd.concat(df_list, ignore_index=True)

    return pivot_data(all_costs, row_label="StartDate", col_label="Category")
