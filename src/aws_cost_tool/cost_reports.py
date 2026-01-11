from functools import reduce

import pandas as pd


def generate_cost_report(
    raw_df: pd.DataFrame, row_label: str, *, selector: int | list[str] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generates a pivoted cost report, filtered a selection of rows per period,
    sorted by the latest costs, with an "Others" row at the bottom. The total
    per column is returned in a separate DataFrame.

    The selector can be either an int to specify the top N, or it can be a list
    of strings for the rows selected.
    """
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    pivoted_df = raw_df.pivot_table(
        index=row_label, columns="StartDate", values="Cost", aggfunc="sum"
    ).fillna(0.0)

    selected = pivoted_df.index.to_list()
    if isinstance(selector, int):
        # Filter to include top N per column.
        top_items = set()
        for column in pivoted_df.columns:
            # Get the top N services for this specific date column
            top_n_for_col = pivoted_df[column].nlargest(selector).index
            top_items.update(top_n_for_col)
        selected = list(top_items)
    elif isinstance(selector, list):
        selected = [i for i in selector if i in pivoted_df.index]

    if not selected:
        raise RuntimeError("No rows selected")

    # Compose the pivot table with Other row.
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


def filter_preserve_date_range(
    df: pd.DataFrame, filters: dict[str, str]
) -> pd.DataFrame:
    """
    A specialized filter for DataFrame rows that ensures all the dates in the
    input DataFrame are present in the filtered data, with the filler rows having
    some appropriate default values:
    - All the columns that are filtered will have the filtered value
    - The Cost column will be 0 in the fillers
    - Everything else will be empty string.

    The rationale for this function is for plotting. As we filter out rows we
    care about, some dates may become missing, and thus the x-axis will change
    depending on the filtering. This function aims to preserve the appearance
    that the all the other dates will have 0 cost. However, to maintain this
    illusion means some of the columns with the default value of "" may not be
    completely correct in all cases.

    The filters is a dictionary of column name as key, and the selected column
    value. For example, if we want to the equivalent of:

    df[(df["A"] == "foo") & (df["B"] == "bar")]

    Then the filters argument will be {"A": "foo", "B": "bar"}

    """
    # First pick out all the unique dates
    date_cols = ["StartDate", "EndDate"]
    all_dates = df[date_cols].drop_duplicates()

    # Filter the rows we want and figure out the missing dates
    criteria = [(df[col] == val) for col, val in filters.items()]
    mask = reduce(lambda x, y: x & y, criteria)
    filtered_df = df[mask].reset_index(drop=True)
    filtered_dates = filtered_df[date_cols].drop_duplicates()

    missing_dates = all_dates.merge(filtered_dates, how="left", indicator=True)
    missing_dates = missing_dates[missing_dates["_merge"] == "left_only"].drop(
        columns="_merge"
    )

    # Now create some a set of the filler rows with the missing dates.
    filler_df = pd.concat([df.iloc[:0], missing_dates])
    cols = set(filler_df.columns) - set(date_cols)
    # Some of the known default values for the filler.
    default_vals = filters | {"Cost": 0}
    for c in cols:
        # Everything else we just fill in empty string
        filler_df[c] = default_vals.get(c, "")

    # Assemble the final thing.
    final_df = pd.concat([filler_df, filtered_df]).sort_values(
        by="StartDate", ascending=True
    )

    return final_df
