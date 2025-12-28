import pandas as pd
import streamlit as st


def render_joint_table(report_df: pd.DataFrame, totals_df: pd.DataFrame):
    """
    Renders two dataframes together.
    - report_df: The sortable service data.
    - totals_df: The anchored, bolded total row.
    """
    if report_df.empty or totals_df.empty:
        st.warning("No data available to display.")
        return

    row_label = str(report_df.index.name) or "Service"

    # Render the main data table, which is sortable by the user.
    dynamic_height = min((len(report_df) + 1) * 35, 500)
    col_config = {row_label: st.column_config.TextColumn(width=200)}
    for col in report_df.columns:
        col_config[col] = st.column_config.NumberColumn(width="small")

    row_style = {
        "background-color": "hsl(210, 20%, 98%)",
        "color": "hsl(210, 10%, 10%)",
    }
    st.dataframe(
        report_df.style.format("${:,.2f}").set_properties(**row_style),  # type: ignore
        width="stretch",
        column_config=col_config,
        height=dynamic_height,
    )

    # Render the Total row (Anchored & Bolded)
    total_style = {
        "font-weight": "500",
        "background-color": "hsl(210, 15%, 90%)",
        "color": "black",
    }
    st.dataframe(
        totals_df.style.format("${:,.2f}").set_properties(**total_style),  # type: ignore
        width="stretch",
        column_config=col_config,
    )
