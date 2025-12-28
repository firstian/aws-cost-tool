import pandas as pd

from .cost_explorer import DateRange, fetch_service_costs, pivot_data


def cost_report_from_raw_df(raw_df: pd.DataFrame, top_n: int):
    """Utility for summarizing the raw DataFrame. Useful on test data as well"""
    if raw_df.empty:
        return pd.DataFrame()

    pivoted_df = pivot_data(raw_df, row_label="Service", col_label="StartDate")

    # Filter to include top N services per period (Column)
    top_services_union = set()
    for column in pivoted_df.columns:
        # Get the top N services for this specific date column
        top_n_for_col = pivoted_df[column].nlargest(top_n).index
        top_services_union.update(top_n_for_col)

    report_df = pivoted_df.loc[list(top_services_union)].copy()

    # Sort rows by the latest date column (descending)
    latest_col = report_df.columns[-1]
    report_df = report_df.sort_values(by=latest_col, ascending=False)

    # Add a Total row by summing the original raw_df.
    totals = raw_df.groupby("StartDate")["Cost"].sum()
    report_df.loc["Total"] = totals

    return report_df


def generate_cost_report(
    ce_client,
    *,
    dates: DateRange,
    tag_key: str = "",
    tag_values: list[str] | None = None,
    granularity: str = "MONTHLY",
    top_n: int = 10
) -> pd.DataFrame:
    """
    Generates a pivoted cost report, filtered for the union of top N services
    per period, sorted by the latest costs, with a total row at the bottom.
    """
    raw_df = fetch_service_costs(
        ce_client,
        dates=dates,
        tag_key=tag_key,
        tag_values=tag_values,
        granularity=granularity,
    )
    return cost_report_from_raw_df(raw_df, top_n)
