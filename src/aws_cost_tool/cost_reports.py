import pandas as pd


def generate_cost_report(
    raw_df: pd.DataFrame, row_label: str, top_n: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generates a pivoted cost report, filtered for the union of top N services
    per period, sorted by the latest costs, with an "Others" row at the bottom.
    The total per column is returned in a separate DataFrame.
    """
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    pivoted_df = raw_df.pivot_table(
        index=row_label, columns="StartDate", values="Cost", aggfunc="sum"
    ).fillna(0.0)

    # Filter to include top N services per column.
    top_items = set()
    for column in pivoted_df.columns:
        # Get the top N services for this specific date column
        top_n_for_col = pivoted_df[column].nlargest(top_n).index
        top_items.update(top_n_for_col)

    # Compose the pivot table with Others row.
    report_df = pivoted_df.loc[list(top_items)].copy()
    totals = raw_df.groupby("StartDate")["Cost"].sum()
    sub_totals = report_df.sum(axis=0)
    report_df.loc["Others"] = totals - sub_totals

    # Sort rows by the latest date column (descending).
    latest_col = report_df.columns[-1]
    report_df = report_df.sort_values(by=latest_col, ascending=False)

    # Add a Total row by summing the original raw_df.
    totals_df = totals.to_frame().T
    totals_df.index = ["Total"]
    totals_df.index.name = row_label

    return report_df, totals_df
