import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal

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

    def __init__(self, *, start: date | str, end: date | str):
        self.start = self._to_date(start)
        self.end = self._to_date(end)
        if self.start >= self.end:
            raise ValueError("start date must be < end date")

    @classmethod
    def from_days(cls, delta: int, *, end: date | str | None = None) -> DateRange:
        """
        Creates a DateRange by looking back 'delta' number of days from an end date.
        The default end date is today.
        """
        if delta <= 0:
            raise ValueError("delta must be > 0")

        end_date = cls._to_date(end) if end else cls._today()
        start_date = end_date - timedelta(days=delta)
        return cls(start=start_date, end=end_date)

    @classmethod
    def from_months(cls, delta: int, *, end: date | str | None = None) -> DateRange:
        """
        Creates a DateRange by looking back 'delta' number of whole months from
        an end date.
        The default end date is today.
        """
        if delta <= 0 or delta > 12:
            raise ValueError("delta must be > 0")

        end_date = cls._to_date(end) if end else cls._today()

        # Calculate the total months since Year 0
        total_months = (end_date.year * 12 + (end_date.month - 1)) - delta

        # Convert back to year and month
        new_year, month_idx = divmod(total_months, 12)
        start_date = date(new_year, month_idx + 1, 1)

        return cls(start=start_date, end=end_date)

    def to_time_period(self) -> dict[str, str]:
        return {
            "Start": self.start.isoformat(),
            "End": self.end.isoformat(),
        }

    @staticmethod
    def _today() -> date:
        # We need this static method in order to mock out today() for unit testing.
        # The trouble is that date itself is implemented in C, so we can't mock
        # it out and still have the isinstance check in _to_date to work. We also
        # can't just mock out today() itself because of the C implementation.
        return date.today()

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

Granularity = Literal["DAILY", "MONTHLY"]


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


def fetch_regions_with_cost(
    client, base_params: dict[str, Any], min_cost: float = 0.01
) -> list[str]:
    """Returns the list of regions with positive cost for the period."""
    params = dict(base_params)
    params["GroupBy"] = [{"Type": "DIMENSION", "Key": "REGION"}]

    regions = set()

    for resp in paginate_ce(client, params):
        for group in resp["ResultsByTime"][0]["Groups"]:
            cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
            if cost > min_cost:
                regions.add(group["Keys"][0])

    return list(regions)


def _fetch_group_by_cost(
    ce_client,
    *,
    dates: DateRange,
    group_by: Sequence[dict[str, Any]],
    filter_expr: dict[str, Any] | None = None,
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
    params = {
        "TimePeriod": dates.to_time_period(),
        "GroupBy": group_by,
        "Granularity": granularity,
        "Metrics": ["UnblendedCost"],
    }
    if filter_expr:
        params["Filter"] = filter_expr

    group_keys = [k["Key"].capitalize() for k in group_by]
    columns = ["StartDate", "EndDate", *group_keys, "Cost"]

    for response in paginate_ce(ce_client, params):
        for period in response["ResultsByTime"]:
            start = period["TimePeriod"]["Start"]
            end = period["TimePeriod"]["End"]
            for group in period["Groups"]:
                values = [
                    start,
                    end,
                    *group["Keys"],
                    float(group["Metrics"]["UnblendedCost"]["Amount"]),
                ]
                results.append(dict(zip(columns, values)))

    df = pd.DataFrame(results)
    if df.empty:
        return pd.DataFrame(columns=columns)

    df["StartDate"] = pd.to_datetime(df["StartDate"]).dt.date
    df["EndDate"] = pd.to_datetime(df["EndDate"]).dt.date
    return df


def fetch_service_costs(
    ce_client,
    *,
    dates: DateRange,
    tag_key: str = "",
    granularity: Granularity = "MONTHLY",
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
            granularity=granularity,
        )

    # Else we want Tag breakdown; iterate by region because CE only limit us to
    # 2 dimensions in group_by,and region has significantly lower cardinality
    # compared to tags.
    base_params = {
        "TimePeriod": dates.to_time_period(),
        "Granularity": granularity,
        "Metrics": ["UnblendedCost"],
    }
    regions = fetch_regions_with_cost(ce_client, base_params, 0.01)
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
            granularity=granularity,
        )
        if not df.empty:
            df["Region"] = region
            df_list.append(df)

        # Rate limit safety: AWS CE API is typically limited to ~1-10 requests/sec
        time.sleep(API_SLEEP_VAL)

    df = pd.concat(df_list)
    columns = ["StartDate", "EndDate", "Tag", "Service", "Region", "Cost"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    # Clean up the columns. Because the utility automatically grab the group key,
    # which happens to be the tag_key value, we need to rename it back to Tag. We
    # also need to strip the tag_key$ prefix that Cost Explorer returns.
    df.rename(columns={tag_key: "Tag"})
    df["Tag"] = df[tag_key].str.removeprefix(f"{tag_key}$")
    df = df[columns]

    return df


def fetch_service_costs_by_usage(
    ce_client,
    *,
    service: str,
    dates: DateRange,
    tag_key: str = "",
    granularity: Granularity = "MONTHLY",
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
            granularity=granularity,
        )

    # Else we want Tag breakdown; iterate by region because CE only limit us to
    # 2 dimensions in group_by,and region has significantly lower cardinality
    # compared to tags.    base_params = {
    base_params = {
        "TimePeriod": dates.to_time_period(),
        "Granularity": granularity,
        "Metrics": ["UnblendedCost"],
    }
    regions = fetch_regions_with_cost(ce_client, base_params, 0.01)
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
            granularity=granularity,
        )
        if not df.empty:
            df["Region"] = region
            df_list.append(df)

        # Rate limit safety: AWS CE API is typically limited to ~1-10 requests/sec
        time.sleep(API_SLEEP_VAL)

    df = pd.concat(df_list)
    columns = ["StartDate", "EndDate", "Tag", "Usage_type", "Region", "Cost"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    # Clean up the columns. Because the utility automatically grab the group key,
    # which happens to be the tag_key value, we need to rename it back to Tag. We
    # also need to strip the tag_key$ prefix that Cost Explorer returns.
    df.rename(columns={tag_key: "Tag"})
    df["Tag"] = df[tag_key].str.removeprefix(f"{tag_key}$")
    df = df[columns]

    return df


## Utilites to transform the raw data into more useful summaries.
def summarize_by_columns(
    df: pd.DataFrame, columns: list[str], threshold: float | None = 0.001
):
    """Utility to aggregate costs based on a list of columns."""
    summary_df = df.groupby(columns, as_index=False)["Cost"].sum()
    if threshold is not None and threshold > 0.0:
        summary_df = summary_df[summary_df["Cost"] >= threshold]
    return summary_df.reset_index()


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
