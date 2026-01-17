from collections.abc import Sequence

import pandas as pd
import plotly.express as px
import streamlit as st

row_style = {
    "background-color": "hsl(210, 20%, 98%)",
    "color": "hsl(210, 10%, 10%)",
}


def df_table(df: pd.DataFrame):
    dynamic_height = min((len(df) + 1) * 35, 318)
    st.dataframe(
        df.style.format("${:,.2f}").set_properties(**row_style),  # type: ignore
        width="content",
        # column_config=col_config,
        height=dynamic_height,
    )


def joint_table(report_df: pd.DataFrame, totals_df: pd.DataFrame):
    """
    Renders two dataframes together.
    - report_df: The sortable service data.
    - totals_df: The anchored, bolded total row.
    """
    if report_df.empty or totals_df.empty:
        st.warning("No data available to display.")
        return

    row_label = str(report_df.index.name) or "Service"

    # These usually came from a pivoted version of the long tables. If the column
    # headers are not JSON serializable, like date, then Streamlit will raise an
    # exception. Make a shallow copy and force them into strings.
    report_df = report_df.rename(columns=str)
    totals_df = totals_df.rename(columns=str)

    # Render the main data table, which is sortable by the user.
    dynamic_height = min((len(report_df) + 1) * 35, 420)
    col_config = {row_label: st.column_config.TextColumn(width=240)}
    for col in report_df.columns:
        col_config[col] = st.column_config.NumberColumn(width="small")

    st.dataframe(
        report_df.style.format("${:,.2f}").set_properties(**row_style),  # type: ignore
        width="stretch",
        column_config=col_config,
        height=dynamic_height,
    )
    # Don't bother doing the total table if there is only one row!
    if len(report_df) <= 1:
        return

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


def download_button(df: pd.DataFrame, help_name: str, file_prefix: str):
    with st.container(horizontal_alignment="right"):
        if st.button(
            "",
            icon=":material/download:",
            key=f"export_{file_prefix}",
            help=f"Download {help_name} CSV",
        ):
            download_dialog(df, file_prefix)


@st.dialog("Download CSV")
def download_dialog(df: pd.DataFrame, name: str):
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
        # If the incoming df has non-standard index, we don't want to lose that
        # column of data, so we have to reset the index to convert it.
        std_index = isinstance(df.index, pd.RangeIndex) and df.index.name is None
        if not std_index:
            df = df.reset_index()

        # Make sure we don't write the excess indices, reset_index actually will
        # generate an index column, so we need to drop it before writing.
        if "index" in df.columns:
            df = df.drop(columns=["index"])

        st.download_button(
            label="Save",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=filename,
            mime="text/csv",
            type="primary",
            width="stretch",
        )


def dropdown_with_all(
    label: str,
    options: Sequence[str],
    *,
    all_label: str = "All",
    empty_label: str = "None",
    key: str | None = None,
    **kwargs,
) -> str | None:
    """
    A more embellished dropdown with the addition of "All" option at the top. It
    also detects empty string in the option and put that in the bottom, and replace
    the displayed value with the empty_label. All the other keyword arguments are
    passed straight to selectbox.
    """
    full_options = [x for x in options if x != ""]
    if len(full_options) < len(options):
        full_options.append("")

    # If we end up with only a single thing, don't both with All.
    if len(full_options) > 1:
        full_options = [all_label] + full_options

    user_format_func = kwargs.pop("format_func", None)

    def formatter(option: str):
        if option == all_label:
            return all_label
        elif option == "":
            return empty_label
        else:
            return user_format_func(option) if user_format_func else option

    return st.selectbox(
        label, options=full_options, index=0, format_func=formatter, key=key, **kwargs
    )


def stack_bar(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    color: str | None = None,
    color_map: dict[str, str] | None = None,
    category_orders: dict[str, list[str]] | None = None,
    height: int = 500,
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

    # Tell Plotly don't be too smart and just treat the dates as categories.
    # Otherwise when there are too few dates, it interpolates inappropriately.
    fig_bar.update_xaxes(type="category")
    fig_bar.update_layout(
        height=height,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,  # Moves it below the X-axis
            xanchor="center",
            x=0.5,
        ),
        margin=dict(t=0, b=0, l=0, r=0),
        xaxis_title=None,
        yaxis_title=y,
    )
    st.plotly_chart(fig_bar, width="stretch")


def pie(
    df: pd.DataFrame | pd.Series,
    *,
    values: str,
    names: str,
    color_map: dict[str, str] | None = None,
    category_orders: dict[str, list[str]] | None = None,
):
    services_pie = px.pie(
        df,
        values=values,
        names=names,
        color=names,
        color_discrete_map=color_map,
        hole=0.4,
        category_orders=category_orders,
    )
    services_pie.update_layout(
        margin=dict(t=10, b=0, l=0, r=0),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
    )
    st.plotly_chart(services_pie, width="stretch")
