import argparse
import os
import sys
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.web.cli as stcli

from app.aws_source import AWSCostSource
from app.interfaces import CostSource
from app.mock_data_source import MockCostSource
from app.ui_components import render_joint_table
from aws_cost_tool.cost_explorer import DateRange
from aws_cost_tool.cost_reports import generate_cost_report

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
        "cost_df": None,
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
    st.session_state.cost_df = None
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


def fetch_data() -> pd.DataFrame:
    """Fetches the cost data and returns the data frame of raw data rows"""
    data_source = get_data_source()

    state = st.session_state
    return data_source.fetch_service_costs(
        dates=DateRange(start=state.start_date, end=state.end_date),
        tag_key=state.tag_key,
        granularity=state.granularity.upper(),
    )


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
            st.markdown(f"**Last Sync:** :grey[{ts}]")

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
            # Vertical alignment trick for the button
            return st.button(
                "Run", type="primary", width="stretch", disabled=dates_invalid
            )


def render_service_cost_report_tab():
    cost_df = st.session_state.cost_df
    if cost_df is None or cost_df.empty:
        st.subheader("Service Cost over Time")
        st.write("No Data")
        return

    start_date = st.session_state.start_date
    end_date = st.session_state.end_date
    st.subheader(f"Service Cost from {start_date} to {end_date}")
    top_n = st.number_input(
        "Top Services", min_value=1, max_value=20, value=6, step=1, width=200
    )
    cost_report_df, total_df = generate_cost_report(cost_df, "Service", top_n)
    render_joint_table(cost_report_df, total_df)


def render_tag_cost_report_tab():
    pass


def render_ui():
    """The main UI layout function."""
    st.set_page_config(layout="wide", page_title="AWS Cost Dashboard")

    # Initialize Globals and default value
    initialize_state()

    render_header()
    run_clicked = render_control_strip()
    if run_clicked:
        try:
            with st.spinner("Fetching data..."):
                st.session_state.cost_df = fetch_data()
                st.session_state.last_fetched = datetime.now()

            st.rerun()
        except ValueError as e:
            st.error(f"Configuration Error: {e}")

    # 2. Setup Tabs
    tab1, tab2 = st.tabs(["Service Cost", "Tagged Cost"])

    with tab1:
        render_service_cost_report_tab()

    with tab2:
        st.write("Placeholder")


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
