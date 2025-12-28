import pandas as pd
import streamlit as st


def render_joint_table(report_df: pd.DataFrame, totals_df: pd.DataFrame):
    """
    Renders two dataframes as a single visual unit.
    - report_df: The sortable service data.
    - totals_df: The anchored, bolded total row.
    """
    if report_df.empty or totals_df.empty:
        st.warning("No data available to display.")
        return

    # 1. Inject CSS to "glue" the two tables together
    # This removes the gap between the first and second dataframe widgets
    st.markdown(
        """
        <style>
            /* Targets the div immediately following a dataframe to remove spacing */
            [data-testid="stDataFrame"] + [data-testid="stDataFrame"] {
                margin-top: -36px;
            }
        </style>
    """,
        unsafe_allow_html=True,
    )
    row_label = str(report_df.index.name) or "Service"

    # 2. Render the main data table (Top N + Others)
    # This remains sortable by the user
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

    # 3. Render the Total row (Anchored & Bolded)
    # We hide the header on this one to make it look like a footer
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
