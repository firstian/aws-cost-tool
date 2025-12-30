import pandas as pd


def generate_cost_report(
    raw_df: pd.DataFrame, row_label: str, *, selector: int | list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generates a pivoted cost report, filtered a selection of rows per period,
    sorted by the latest costs, with an "Other" row at the bottom. The total
    per column is returned in a separate DataFrame.

    The selector can be either an int to specify the top N, or it can be a list
    of strings for the rows selected.
    """
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    pivoted_df = raw_df.pivot_table(
        index=row_label, columns="StartDate", values="Cost", aggfunc="sum"
    ).fillna(0.0)

    selected = []
    if isinstance(selector, int):
        # Filter to include top N per column.
        top_items = set()
        for column in pivoted_df.columns:
            # Get the top N services for this specific date column
            top_n_for_col = pivoted_df[column].nlargest(selector).index
            top_items.update(top_n_for_col)
        selected = list(top_items)
    else:
        selected = [i for i in selector if i in pivoted_df.index]

    if not selected:
        raise RuntimeError("No rows selected")

    # Compose the pivot table with Others row.
    report_df = pivoted_df.loc[selected].copy()
    totals = raw_df.groupby("StartDate")["Cost"].sum()
    sub_totals = report_df.sum(axis=0)
    remainders = totals - sub_totals
    remainders = remainders.where(remainders.abs() >= 0.01, 0)
    if not (remainders == 0).all():
        report_df.loc["Other"] = remainders

    # Sort rows by the latest date column (descending).
    if not report_df.empty:
        latest_col = report_df.columns[-1]
        report_df = report_df.sort_values(by=latest_col, ascending=False)

    # Add a Total row by summing the original raw_df.
    totals_df = totals.to_frame().T
    totals_df.index = ["Total"]
    totals_df.index.name = row_label

    return report_df, totals_df
