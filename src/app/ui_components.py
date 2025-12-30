import pandas as pd
import plotly.express as px
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


@st.dialog("Download CSV")
def render_download_dialog(df: pd.DataFrame, name: str):
    if df.empty:
        return
    state = st.session_state
    filename = st.text_input(
        "Filename",
        value=f"{name}_{state.start_date}_to_{state.end_date}.csv",
        help="Enter the desired filename (with .csv extension)",
    )
    # Ensure .csv extension
    if not filename.endswith(".csv"):
        st.warning("⚠️ Filename should end with .csv")

    col1, col2 = st.columns([1, 1])

    with col1:
        if st.button("Cancel", width="stretch"):
            st.rerun()

    with col2:
        csv = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Save",
            data=csv,
            file_name=filename,
            mime="text/csv",
            type="primary",
            width="stretch",
        )


def render_stack_bar(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    color: str | None = None,
    color_map: dict[str, str] | None = None,
    category_orders: dict[str, list[str]] | None = None,
):
    if df.empty:
        return
    fig_bar = px.bar(
        df,
        x=x,
        y=y,
        color=color,  # This is the key change
        color_discrete_map=color_map,
        category_orders=category_orders,
    )

    fig_bar.update_layout(
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,  # Moves it below the X-axis
            xanchor="center",
            x=0.5,
        ),
        margin=dict(t=10, b=50, l=10, r=10),
        xaxis_title=None,
        yaxis_title=y,
    )
    st.plotly_chart(fig_bar, width="stretch")
