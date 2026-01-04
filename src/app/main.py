import argparse
import logging
import os
import sys
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.web.cli as stcli

import app.ui_components as ui
import aws_cost_tool.service_loader as service_loader
from app.aws_source import AWSCostSource
from app.interfaces import CostSource
from app.mock_data_source import MockCostSource
from app.sql_tab import render_sql_sandbox
from aws_cost_tool.cost_explorer import DateRange, summarize_by_columns
from aws_cost_tool.cost_reports import generate_cost_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more details
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],  # prints to console (where streamlit runs)
)

logger = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="AWS Cost Explorer")


class ReportChoice(StrEnum):
    """Enum to be used with the dropdown"""

    LAST_7_DAYS = "Last 7 days"
    LAST_30_DAYS = "Last 30 days"
    LAST_6_MONTHS = "Last 6 months"
    LAST_12_MONTHS = "Last 12 months"
    CUSTOM = "Custom"

    def granularity(self) -> str:
        match self:
            case ReportChoice.LAST_7_DAYS | ReportChoice.LAST_30_DAYS:
                return "DAILY"
            case ReportChoice.LAST_6_MONTHS | ReportChoice.LAST_12_MONTHS:
                return "MONTHLY"
            case ReportChoice.CUSTOM:
                return ""  # We don't know, so let someone else decide.


@st.cache_resource
def initialize_services():
    """Load the service plugins once at startup and keep them in memory."""
    service_loader.load_services("aws_cost_tool.services")


def initialize_state():
    """Initializes session state variables if they don't exist."""
    dr = DateRange.from_days(7)
    default_choice = ReportChoice.LAST_7_DAYS
    defaults = {
        "profile": os.environ.get("AWS_PROFILE"),
        "tag_key": os.environ.get("TAG_KEY") or "",
        "end_date": dr.end,
        "start_date": dr.start,
        "report_choice": default_choice,
        "granularity": default_choice.granularity().capitalize(),
        "cost_data": {},
        "last_fetched": None,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def get_data_source() -> CostSource:
    if st.session_state.profile == "mock_data":
        return MockCostSource()

    return AWSCostSource(st.session_state.profile)


def on_change_reset_data():
    """Callback: if parameters change, the current cached data is no longer valid."""
    st.session_state.cost_data = {}
    st.session_state.last_fetched = None


def on_dropdown_change():
    """Callback for when the canned report dropdown changes."""
    on_change_reset_data()

    choice = st.session_state.report_choice
    dr: DateRange | None = None
    match choice:
        case ReportChoice.LAST_7_DAYS:
            dr = DateRange.from_days(7)
        case ReportChoice.LAST_30_DAYS:
            dr = DateRange.from_days(30)
        case ReportChoice.LAST_6_MONTHS:
            dr = DateRange.from_months(6)
        case ReportChoice.LAST_12_MONTHS:
            dr = DateRange.from_months(12)
        case ReportChoice.CUSTOM:
            pass  # Handled by date controls implicitly.
        case _:
            raise ValueError(f"Unhandled Choice: {choice}")

    if dr is not None:
        st.session_state.start_date = dr.start
        st.session_state.end_date = dr.end
        st.session_state.granularity = choice.granularity().capitalize()


def on_change_from_fixed_choices():
    """Callback for when inputs are modified manually."""
    on_change_reset_data()
    st.session_state.report_choice = ReportChoice.CUSTOM


def fetch_cost_data(key: str):
    """Fetches the cost data and returns the data frame of raw data rows"""

    state = st.session_state
    if key != "cost_df" and state.cost_data.get("cost_df") is None:
        raise RuntimeError("Inconsistent state: cost_df missing")

    df = state.cost_data.get(key)
    if df is not None:
        return df

    try:
        logger.info(f"Start fetching {key=}")
        data_source = get_data_source()
        with st.spinner("Fetching data..."):
            if key == "cost_df":
                df = data_source.fetch_service_costs(
                    dates=DateRange(start=state.start_date, end=state.end_date),
                    tag_key=state.tag_key,
                    granularity=state.granularity.upper(),
                )
            else:
                df = data_source.fetch_service_costs_by_usage(
                    service=key,
                    dates=DateRange(start=state.start_date, end=state.end_date),
                    tag_key=state.tag_key,
                    granularity=state.granularity.upper(),
                )
                service = service_loader.get_service(key)
                if service is not None:
                    df = service.categorize_usage(df)

        # Update timestamp for fetching.
        state.cost_data[key] = df
        state.last_fetched = datetime.now()
        st.rerun()
    except ValueError as e:  # Don't catch all, otherwise st.rerun will fail.
        st.error(f"Data fetch Error: {e}")


def render_header():
    st.title("AWS Cost Dashboard")

    profile = st.session_state.profile
    profile_txt = f"orange[{profile}]" if profile else "grey[default]"
    col_a, col_b = st.columns([1, 1])

    with col_a:
        st.markdown(f"**AWS Profile:** :{profile_txt}")
    with col_b:
        if st.session_state.last_fetched:
            ts = st.session_state.last_fetched.strftime("%H:%M:%S")
            st.markdown(
                f"""
                <div style='text-align: right;'>
                    <b>Last Sync:</b> <span style='color: grey;'>{ts}</span>
                </div>""",
                unsafe_allow_html=True,
            )
    if st.session_state.end_date <= st.session_state.start_date:
        st.error("End date must be greater than Start date!")


def render_control_strip() -> bool:
    """Renders the control strip, and returns whether the button is clicked."""
    dates_invalid = st.session_state.end_date <= st.session_state.start_date

    with st.container(border=True):
        dropdown, start_date, end_date, granularity, run_btn = st.columns(
            [1.4, 1, 1, 1, 1], vertical_alignment="bottom"
        )

        with dropdown:
            st.selectbox(
                "Report Period",
                options=list(ReportChoice),
                format_func=lambda c: c.value,
                key="report_choice",
                on_change=on_dropdown_change,
            )

        with start_date:
            st.date_input(
                "Start Date",
                key="start_date",
                on_change=on_change_from_fixed_choices,
            )

        with end_date:
            st.date_input(
                "End Date", key="end_date", on_change=on_change_from_fixed_choices
            )

        with granularity:
            st.markdown(
                """
                <style>
                /* Match segmented control height to other inputs */
                div[data-baseweb="button-group"] {
                    display: flex;
                    flex-wrap: nowrap;
                    & button {
                        height: 40px;
                    }
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.segmented_control(
                "Granularity",
                options=["Daily", "Monthly"],
                key="granularity",
                on_change=on_change_from_fixed_choices,
            )
        with run_btn:
            return st.button(
                "Run",
                key="run_btn",
                type="primary",
                width="stretch",
                disabled=dates_invalid,
            )


def render_filter_strip(df: pd.DataFrame, key_prefix: str = "") -> pd.DataFrame:
    """
    Renders the filter strip with region and tag options. It returns the filtered
    DataFrame based on the selection.
    Note that the caller need to provide a distinct key_prefix if the filter
    strip is used in multiple tabs to avoid key collisions.
    """
    ALL_REGIONS = "All regions"
    ALL_TAGS = "All Tags"
    container = st.container()
    with container:
        col1, col2 = st.columns([1, 3])

        with col1:
            region_list = sorted(df["Region"].unique())
            region = ui.dropdown_with_all(
                "Region",
                options=region_list,
                all_label=ALL_REGIONS,
                key=f"{key_prefix}_region",
                label_visibility="collapsed",
                width=200,
            )

        with col2:
            # We sort the tag by cost so the big spenders are at the top.
            sorted_tags = (
                df.groupby("Tag")["Cost"]
                .sum()
                .sort_values(ascending=False)
                .index.tolist()
            )
            tag = ui.dropdown_with_all(
                "Tags",
                options=sorted_tags,
                all_label=ALL_TAGS,
                empty_label="Untagged",
                key=f"{key_prefix}_tag",
                label_visibility="collapsed",
                help="Tags sorted in descending order in cost",
            )

    filtered_df = df
    if region is not None and region != ALL_REGIONS:
        filtered_df = filtered_df[filtered_df["Region"] == region]

    if tag is not None and tag != ALL_TAGS:
        filtered_df = filtered_df[filtered_df["Tag"] == tag]

    return filtered_df


@st.fragment
def render_service_cost_report_tab(run_fetch: bool):
    # Run fetch inside the tab so that the progress spinner is displayed within
    # the tab.
    if run_fetch:
        fetch_cost_data("cost_df")

    cost_df = st.session_state.cost_data.get("cost_df")
    if cost_df is None or cost_df.empty:
        st.write("No Data")
        return

    service_cnt = len(cost_df["Service"].unique())
    col1, col2 = st.columns([8, 1], vertical_alignment="bottom")
    with col1:
        top_n = st.number_input(
            "Top Services",
            min_value=1,
            max_value=service_cnt,
            value=6,
            step=1,
            width=200,
        )

    with col2:
        ui.download_button(cost_df, "service cost", "aws_cost")

    cost_report_df, total_df = generate_cost_report(cost_df, "Service", selector=top_n)
    ui.joint_table(cost_report_df, total_df)
    # Plotly wants the unpivoted data for plotting.
    melted_df = cost_report_df.reset_index().melt(
        id_vars="Service", var_name="StartDate", value_name="Cost"
    )

    # Plot the stacked bar chart for the services over time.
    services = sorted(cost_report_df.index)
    sort_order = [s for s in services if s != "Other"] + (
        ["Other"] if "Other" in services else []
    )
    ui.stack_bar(
        melted_df,
        x="StartDate",
        y="Cost",
        color="Service",  # This is the key change
        category_orders={"Service": sort_order},
    )


@st.fragment
def render_tag_cost_report_tab():
    tag_key = st.session_state.tag_key
    if not tag_key:
        st.warning("No tag-key: provide --tag-key flag to enable tag break down.")
        return

    cost_df = st.session_state.cost_data.get("cost_df")
    if cost_df is None or cost_df.empty:
        st.write("No Data")
        return

    label_cnt = len(cost_df["Tag"].unique())
    top_n = st.number_input(
        f"Top **{tag_key}** Tags",
        min_value=1,
        max_value=label_cnt,
        value=min(label_cnt, 2),
        step=1,
        width=200,
    )

    st.caption("Total Cost by Tag")
    cost_report_df, total_df = generate_cost_report(cost_df, "Tag", selector=top_n)
    cost_report_df.rename(index={"": "Untagged"}, inplace=True)
    ui.joint_table(cost_report_df, total_df)

    st.divider()
    selected_tag = st.selectbox(
        "Service breakdown for Tag",
        # label_visibility="collapsed",
        options=sorted(list(cost_report_df.index)),
        index=None,
        placeholder="Select a tag...",
        width=400,
        key="selected_tag",
    )

    if selected_tag is None:
        return

    # Restore the tag value.
    if selected_tag == "Untagged":
        selected_tag = ""

    render_tagged_breakdown_charts(selected_tag, cost_df)


def render_tagged_breakdown_charts(selected_tag: str, cost_df: pd.DataFrame):
    # TODO: Deal with the hardcoded top_n == 4.
    # Use the cost report structure to reuse the top N + Other logic.
    pivot_df, total_df = generate_cost_report(
        cost_df[cost_df["Tag"] == selected_tag], "Service", selector=4
    )

    # Plotly wants the unpivoted data for plotting.
    melted_df = pivot_df.reset_index().melt(
        id_vars="Service", var_name="StartDate", value_name="Cost"
    )
    ui.joint_table(pivot_df, total_df)

    # Make sure we use a consistent colormap for service
    services = sorted(pivot_df.index)
    colors = px.colors.qualitative.Plotly
    color_map = {service: colors[i % len(colors)] for i, service in enumerate(services)}

    # Plot the stacked bar chart for the selected tag over time.
    sort_order = [s for s in services if s != "Other"] + (
        ["Other"] if "Other" in services else []
    )
    ui.stack_bar(
        melted_df,
        x="StartDate",
        y="Cost",
        color="Service",  # This is the key change
        color_map=color_map,
        category_orders={"Service": sort_order},
    )

    # Set up the selectbox for the time period for further breakdown.
    time_periods = sorted(pivot_df.columns.tolist(), reverse=True)
    selected_period = st.selectbox(
        "Select Time Period:",
        label_visibility="collapsed",
        options=time_periods,
        index=0,
        width=300,
        key="selected_time_period",
    )

    # Region column is needed from the full cost_df for region breakdown.
    filtered_df = cost_df[
        (cost_df["StartDate"] == selected_period) & (cost_df["Tag"] == selected_tag)
    ]
    region_df = filtered_df.groupby(["Region"], as_index=False)["Cost"].sum()

    col_left, col_right = st.columns([1, 1])
    with col_left:
        st.caption("Services")
        ui.pie(
            melted_df[melted_df["StartDate"] == selected_period],
            values="Cost",
            names="Service",
            color_map=color_map,
            category_orders={"Service": sort_order},
        )
    with col_right:
        st.caption("Regions")
        ui.pie(
            region_df,
            values="Cost",
            names="Region",  # Pie slices are Services
        )


@st.fragment
def render_service_usage_report_tab():
    cost_df = st.session_state.cost_data.get("cost_df")
    if cost_df is None or cost_df.empty:
        st.write("No Data")
        return

    col1, col2, col3 = st.columns([4, 6, 1], vertical_alignment="bottom")
    with col1:
        selected_name = st.selectbox(
            "Service",
            label_visibility="collapsed",
            options=service_loader.services_names(),
            index=None,
            placeholder="Select a service...",
            width=500,
            key="selected_service",
        )
        if selected_name is None:
            return

    shortname = service_loader.get_service_shortname(selected_name)

    fetch_cost_data(selected_name)
    service_df = st.session_state.cost_data[selected_name]

    with col2:
        filtered_df = render_filter_strip(service_df, key_prefix="service_usage")

    with col3:
        file_prefix = service_loader.get_file_prefix(selected_name)
        ui.download_button(filtered_df, f"{shortname} usage", file_prefix)

    category_df = filtered_df.groupby([pd.Grouper(level="Category"), "StartDate"])[
        "Cost"
    ].sum()
    category_df = category_df.reset_index()
    pivot_df, total_df = generate_cost_report(category_df, "Category")
    st.caption("Service Usage breakdown")
    ui.joint_table(pivot_df, total_df)

    st.caption(f"{shortname} breakdown")
    ui.stack_bar(category_df, x="StartDate", y="Cost", color="Category")

    # Only plot more graphs if there are subtypes to handle.
    if isinstance(filtered_df.index, pd.MultiIndex):
        for t in filtered_df.index.levels[0].to_list():
            if t != "Other":
                render_subtype_stack_bar(filtered_df, t)


def render_subtype_stack_bar(df: pd.DataFrame, key: str):
    st.caption(f"{key} cost breakdown")
    if key not in df.index:
        st.write("No data")
        return

    dt_df = summarize_by_columns(df.loc[[key]], ["Subtype", "StartDate"])
    ui.stack_bar(dt_df, x="StartDate", y="Cost", color="Subtype")


def render_ui():
    """The main UI layout function."""
    st.set_page_config(layout="wide", page_title="AWS Cost Dashboard")

    # Initialize Globals and default value
    initialize_state()
    initialize_services()

    render_header()
    run_clicked = render_control_strip()

    # Setup Tabs
    (
        service_tab,
        tagged_tab,
        service_usage_tab,
        sql_tab,
    ) = st.tabs(["Service Cost", "Tagged Cost", "Service Usage", "SQL Sandbox"])

    with service_tab:
        render_service_cost_report_tab(run_clicked)

    with tagged_tab:
        render_tag_cost_report_tab()

    with service_usage_tab:
        render_service_usage_report_tab()

    with sql_tab:
        render_sql_sandbox()


def start_app():
    """Entry point for [project.scripts] in pyproject.toml."""
    parser = argparse.ArgumentParser(description="AWS Cost Explorer Report")
    parser.add_argument(
        "--profile", type=str, help="AWS CLI profile name", default=None
    )
    parser.add_argument(
        "--tag-key", type=str, help="The tag key used to find tags", default=""
    )
    args, unknown = parser.parse_known_args()

    # Pass the profile to the Streamlit app via an envvar
    if args.profile:
        os.environ["AWS_PROFILE"] = args.profile

    if args.tag_key:
        os.environ["TAG_KEY"] = args.tag_key

    this_file = str(Path(__file__).resolve())
    sys.argv = ["streamlit", "run", this_file]
    sys.exit(stcli.main())


if __name__ == "__main__":
    render_ui()
