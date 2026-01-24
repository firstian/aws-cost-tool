import logging
import time
from collections.abc import Iterator, Sequence
from typing import Any, cast

import pandas as pd

from aws_cost_tool.ce_types import CostMetric, DateRange, Granularity

API_SLEEP_VAL = 0.2

logger = logging.getLogger(__name__)


## Utilities to discover usable tag key and values
def get_tag_keys(ce_client, *, dates: DateRange) -> list[str]:
    """Get the list of all tag keys."""
    try:
        response = ce_client.get_tags(TimePeriod=dates.to_time_period())
        return response.get("Tags", [])
    except Exception as e:
        logger.error(f"Error fetching tag keys: {e}")

    return []


def get_tags_for_key(ce_client, *, tag_key: str, dates: DateRange) -> list[str]:
    """Get the list of all tag values for a given tag key."""
    tag_keys = set()

    try:
        response = ce_client.get_tags(TimePeriod=dates.to_time_period(), TagKey=tag_key)
        tag_keys.update(response.get("Tags", []))
    except Exception as e:
        logger.error(f"Error fetching tag keys: {e}")

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


def paginate_ce(client, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """A generator that handles pagination of get_cost_and_usage."""
    while True:
        response = client.get_cost_and_usage(**params)
        yield response
        token = response.get("NextPageToken")
        if not token:
            break
        params = dict(params)
        params["NextPageToken"] = token


def fetch_active_regions(
    client,
    dates: DateRange,
    granularity: Granularity = "MONTHLY",
    min_cost: float = 0.01,
) -> list[str]:
    """Returns the list of regions with positive cost for the period."""
    # We only want to find regions we spent money. The cost metric doesn't matter.
    cost_metric = "UnblendedCost"
    params = {
        "TimePeriod": dates.to_time_period(),
        "Granularity": granularity,
        "Metrics": [cost_metric],
        "GroupBy": [{"Type": "DIMENSION", "Key": "REGION"}],
    }

    regions = set()

    for resp in paginate_ce(client, params):
        for group in resp["ResultsByTime"][0]["Groups"]:
            cost = float(group["Metrics"][cost_metric]["Amount"])
            if cost > min_cost:
                regions.add(group["Keys"][0])

    return list(regions)


def json_to_df(
    response: dict[str, Any],
    group_by: Sequence[dict[str, Any]],
    cost_metric: CostMetric,
) -> pd.DataFrame:
    """
    Formats the json cost response into a DataFrame. Each entry has a start and
    end date.

    Returns a DataFrame has the following columns:
    StartDate, EndDate, <group_by> Cost

    where <group_by> is the capitalized version of the argument.
    """
    results = []
    group_keys = [str(k["Key"]).capitalize() for k in group_by]
    columns = pd.Index(["StartDate", "EndDate", *group_keys, "Cost"])
    for period in response["ResultsByTime"]:
        start = period["TimePeriod"]["Start"]
        end = period["TimePeriod"]["End"]
        for group in period["Groups"]:
            values = [
                start,
                end,
                *group["Keys"],
                float(group["Metrics"][cost_metric]["Amount"]),
            ]
            results.append(dict(zip(columns, values)))

    if not results:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(results)
    df["StartDate"] = pd.to_datetime(df["StartDate"]).dt.date
    df["EndDate"] = pd.to_datetime(df["EndDate"]).dt.date
    return df


def _fetch_group_by_cost(
    ce_client,
    *,
    dates: DateRange,
    group_by: Sequence[dict[str, Any]],
    filter_expr: dict[str, Any] | None = None,
    cost_metric: CostMetric,
    granularity: Granularity,
) -> pd.DataFrame:
    """
    Fetches cost data with the specified group_by dimenion and filter. Each entry
    has a start and end date. This is the main worker function used to implement
    all the rest of the higher level data fetcher.

    Returns a DataFrame has the following columns:
    StartDate, EndDate, <group_by> Cost

    where <group_by> is the capitalized version of the argument.
    """
    results = []
    group_keys = [str(k["Key"]).capitalize() for k in group_by]
    columns = pd.Index(["StartDate", "EndDate", *group_keys, "Cost"])
    params = {
        "TimePeriod": dates.to_time_period(),
        "GroupBy": group_by,
        "Granularity": granularity,
        "Metrics": [cost_metric],
    }
    if filter_expr:
        params["Filter"] = filter_expr

    for response in paginate_ce(ce_client, params):
        results.append(json_to_df(response, group_by, cost_metric))

    if not results:
        return pd.DataFrame(columns=columns)

    return cast(pd.DataFrame, pd.concat(results, ignore_index=True))


def fetch_service_costs(
    ce_client,
    *,
    dates: DateRange,
    tag_key: str = "",
    cost_metric: CostMetric,
    granularity: Granularity,
) -> pd.DataFrame:
    """
    Fetches the costs broken out by service, Tag, and Region. Returns a DataFrame
    with columns: StartDate, EndDate, Tag, Service, Region Cost

    If no tag_key is passed, then everything will be aggregated, and the returned
    DataFrame has no Tag column.
    """
    # We usually don't care about Marketplace spending in cost monitoring.
    exclude_filter = {
        "Not": {"Dimensions": {"Key": "BILLING_ENTITY", "Values": ["AWS Marketplace"]}}
    }

    #  No tag breakdown
    if not tag_key:
        return _fetch_group_by_cost(
            ce_client,
            dates=dates,
            group_by=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "REGION"},
            ],
            filter_expr=exclude_filter,
            cost_metric=cost_metric,
            granularity=granularity,
        )

    # Else we want Tag breakdown; iterate by region because CE only limit us to
    # 2 dimensions in group_by,and region has significantly lower cardinality
    # compared to tags.
    regions = fetch_active_regions(ce_client, dates, granularity, 0.01)
    df_list = []
    for region in regions:
        region_filter = {"Dimensions": {"Key": "REGION", "Values": [region]}}
        df = _fetch_group_by_cost(
            ce_client,
            dates=dates,
            group_by=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "TAG", "Key": tag_key},
            ],
            filter_expr={"And": [region_filter, exclude_filter]},
            cost_metric=cost_metric,
            granularity=granularity,
        )
        if not df.empty:
            df["Region"] = region
            df_list.append(df)

        # Rate limit safety: AWS CE API is typically limited to ~1-10 requests/sec
        time.sleep(API_SLEEP_VAL)

    columns = pd.Index(["StartDate", "EndDate", "Tag", "Service", "Region", "Cost"])
    if not df_list:
        return pd.DataFrame(columns=columns)

    df = cast(pd.DataFrame, pd.concat(df_list, ignore_index=True))
    if df.empty:
        return pd.DataFrame(columns=columns)

    # Clean up the columns. Because the utility automatically grab the group key,
    # which happens to be the tag_key value, we need to rename it back to Tag. We
    # also need to strip the tag_key$ prefix that Cost Explorer returns.
    df["Tag"] = df[tag_key].str.removeprefix(f"{tag_key}$")
    df = df.drop(columns=[tag_key])
    df = cast(pd.DataFrame, df.loc[:, columns])

    return df


def fetch_service_costs_by_usage(
    ce_client,
    *,
    service: str,
    dates: DateRange,
    tag_key: str = "",
    cost_metric: CostMetric,
    granularity: Granularity,
) -> pd.DataFrame:
    """
    Fetches service costs broken out by Tag, Region, and Usage Type. Returns a
    DataFrame with columns: StartDate, EndDate, Tag, Usage_type, Region, Cost

    If no tag_key is passed, then everything will be aggregated, and the returned
    DataFrame has no Tag column.
    """
    service_filter = {"Dimensions": {"Key": "SERVICE", "Values": [service]}}
    group_by = "USAGE_TYPE"

    # No tag breakdown
    if not tag_key:
        return _fetch_group_by_cost(
            ce_client,
            dates=dates,
            group_by=[
                {"Type": "DIMENSION", "Key": group_by.upper()},
                {"Type": "DIMENSION", "Key": "REGION"},
            ],
            filter_expr=service_filter,
            cost_metric=cost_metric,
            granularity=granularity,
        )

    # Else we want Tag breakdown; iterate by region because CE only limit us to
    # 2 dimensions in group_by,and region has significantly lower cardinality
    # compared to tags.
    regions = fetch_active_regions(ce_client, dates, granularity, 0.01)
    df_list = []
    for region in regions:
        region_filter = {"Dimensions": {"Key": "REGION", "Values": [region]}}
        df = _fetch_group_by_cost(
            ce_client,
            dates=dates,
            group_by=[
                {"Type": "DIMENSION", "Key": group_by.upper()},
                {"Type": "TAG", "Key": tag_key},
            ],
            filter_expr={"And": [service_filter, region_filter]},
            cost_metric=cost_metric,
            granularity=granularity,
        )
        if not df.empty:
            df["Region"] = region
            df_list.append(df)

        # Rate limit safety: AWS CE API is typically limited to ~1-10 requests/sec
        time.sleep(API_SLEEP_VAL)

    columns = pd.Index(
        [
            "StartDate",
            "EndDate",
            "Tag",
            "Usage_type",
            "Region",
            "Cost",
        ]
    )
    if not df_list:
        return pd.DataFrame(columns=columns)

    df = cast(pd.DataFrame, pd.concat(df_list, ignore_index=True))
    if df.empty:
        return pd.DataFrame(columns=columns)

    # Clean up the columns. Because the utility automatically grab the group key,
    # which happens to be the tag_key value, we need to rename it back to Tag. We
    # also need to strip the tag_key$ prefix that Cost Explorer returns.
    df["Tag"] = df[tag_key].str.removeprefix(f"{tag_key}$")
    df = df.drop(columns=[tag_key])
    df = cast(pd.DataFrame, df.loc[:, columns])

    return df


## Utilites to transform the raw data into more useful summaries.
def summarize_by_columns(
    df: pd.DataFrame, columns: list[str], threshold: float | None = 0.001
) -> pd.DataFrame:
    """Utility to aggregate costs based on a list of columns."""
    summary_df = df.groupby(columns, as_index=False)["Cost"].sum()
    if threshold is not None and threshold > 0.0:
        summary_df = summary_df[summary_df["Cost"] >= threshold]
    return summary_df.reset_index(drop=True)  # type: ignore


def pivot_data(
    df: pd.DataFrame, *, row_label: str, col_label: str, threshold: float = 0.001
) -> pd.DataFrame:
    """
    Returns a summary of services cost for each unit of time, pivoted based on
    the specified row and column. This is primarily useful for human consumption.
    """
    summary = summarize_by_columns(df, [row_label, col_label], threshold)
    return summary.pivot_table(
        index=row_label, columns=col_label, values="Cost", aggfunc="sum"
    ).fillna(0.0)
