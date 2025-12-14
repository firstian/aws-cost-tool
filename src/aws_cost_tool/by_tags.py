from dataclasses import dataclass
from datetime import date, datetime, timedelta

import boto3
import pandas as pd


@dataclass
class CostRecord:
    service: str
    region: str
    cost: float
    tag_key: str | None = None
    tag_value: str | None = None


def create_ce_client(profile_name: str | None = None, region: str = "us-east-1"):
    """Create a Cost Explorer client, defaults to us-east-1 where CE is hosted."""
    session = boto3.Session(profile_name=profile_name)
    return session.client("ce", region_name=region)


def _make_time_period(start_date: date, end_date: date) -> dict[str, str]:
    return {
        "Start": start_date.strftime("%Y-%m-%d"),
        "End": end_date.strftime("%Y-%m-%d"),
    }


def get_all_tags(ce_client, *, start_date: date, end_date: date) -> list[str]:
    """Get the list of all available cost allocation tags."""
    tag_keys = set()
    next_token = ""

    while True:
        try:
            response = ce_client.get_tags(
                TimePeriod=_make_time_period(start_date, end_date),
                TagKey="*User-Defined-Tags*",  # Filter for user-defined tags
                NextPageToken=next_token,
            )
            tag_keys.update(response.get("Tags", []))

            next_token = response.get("NextPageToken")
            if not next_token:
                break
        except Exception as e:
            print(f"Error fetching tag keys: {e}")
            break

    # Cost Explorer automatically prepends 'user:' to activated tags
    cleaned_keys = [key for key in tag_keys if not key.startswith("aws:")]

    return cleaned_keys


def get_costs_by_tag(
    ce_client, *, tag_key: str | None = None, start_date: date, end_date: date
) -> list[CostRecord]:
    """Query costs broken down by a specific tag, service, and region."""
    groupby_param = [
        {"Type": "DIMENSION", "Key": "SERVICE"},
        {"Type": "DIMENSION", "Key": "REGION"},
    ]
    if tag_key == "":
        tag_key = None

    if tag_key is not None:
        groupby_param.append(
            {"Type": "TAG", "Key": tag_key},
        )
    response = ce_client.get_cost_and_usage(
        TimePeriod=_make_time_period(start_date, end_date),
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=groupby_param,
    )

    results = []
    for time_period in response["ResultsByTime"]:
        for group in time_period["Groups"]:
            service = group["Keys"][0]
            region = group["Keys"][1]
            tag_value = group["Keys"][2] if tag_key and group["Keys"][2] else None
            cost = float(group["Metrics"]["UnblendedCost"]["Amount"])

            if cost > 0:
                results.append(
                    CostRecord(
                        tag_key=tag_key,
                        tag_value=tag_value,
                        service=service,
                        region=region,
                        cost=cost,
                    )
                )

    return results


def analyze_costs(
    ce_client,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """Analyze AWS costs broken down by tags, service, and region."""

    # Set date range
    if end_date is None:
        end_date = datetime.now().date()

    if start_date is None:
        start_date = end_date - timedelta(days=30)

    # Get all available tags
    tags = get_all_tags(ce_client, start_date=start_date, end_date=end_date)

    # Collect all cost data
    all_costs = []

    # Query costs for each tag
    for tag in tags:
        tag_costs = get_costs_by_tag(
            ce_client, tag_key=tag, start_date=start_date, end_date=end_date
        )
        all_costs.extend(tag_costs)

    # Get total costs by service and region
    total_costs_by_service_region = get_total_costs(ce_client, start_str, end_str)

    # Create dataframe from tagged costs
    if all_costs:
        df_tagged = pd.DataFrame(all_costs)

        # Aggregate costs by tag_key, tag_value, service, and region
        df_tagged = (
            df_tagged.groupby(
                ["tag_key", "tag_value", "service", "region"], dropna=False
            )["cost"]
            .sum()
            .reset_index()
        )

        # Calculate total tagged cost per service/region combination
        tagged_totals = (
            df_tagged.groupby(["service", "region"])["cost"].sum().reset_index()
        )
        tagged_totals.columns = ["service", "region", "tagged_cost"]
    else:
        df_tagged = pd.DataFrame(
            columns=["tag_key", "tag_value", "service", "region", "cost"]
        )
        tagged_totals = pd.DataFrame(columns=["service", "region", "tagged_cost"])

    # Identify untagged costs
    df_total = pd.DataFrame(total_costs_by_service_region)
    if not df_total.empty and not tagged_totals.empty:
        df_merged = df_total.merge(tagged_totals, on=["service", "region"], how="left")
        df_merged["tagged_cost"] = df_merged["tagged_cost"].fillna(0)
        df_merged["untagged_cost"] = df_merged["total_cost"] - df_merged["tagged_cost"]

        # Create untagged entries
        untagged_entries = df_merged[df_merged["untagged_cost"] > 0.01][
            ["service", "region", "untagged_cost"]
        ]
        untagged_entries.columns = ["service", "region", "cost"]
        untagged_entries["tag_key"] = None
        untagged_entries["tag_value"] = None
    elif not df_total.empty:
        # All costs are untagged
        untagged_entries = df_total[["service", "region", "total_cost"]].copy()
        untagged_entries.columns = ["service", "region", "cost"]
        untagged_entries["tag_key"] = None
        untagged_entries["tag_value"] = None
    else:
        untagged_entries = pd.DataFrame(
            columns=["tag_key", "tag_value", "service", "region", "cost"]
        )

    # Combine tagged and untagged data
    final_df = pd.concat([df_tagged, untagged_entries], ignore_index=True)

    # Reorder columns for clarity
    final_df = final_df[["tag_key", "tag_value", "service", "region", "cost"]]

    # Sort by cost descending
    final_df = final_df.sort_values("cost", ascending=False).reset_index(drop=True)

    return final_df
