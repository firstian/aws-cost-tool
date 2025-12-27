import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import streamlit as st
import streamlit.web.cli as stcli

from aws_cost_tool.client import create_ce_client
from aws_cost_tool.cost_reports import DateRange, generate_cost_report

st.set_page_config(layout="wide", page_title="AWS Cost Explorer")


def initialize_state():
    """Initializes session state variables if they don't exist."""
    if "end_date" not in st.session_state:
        st.session_state.end_date = date.today()
    if "start_date" not in st.session_state:
        st.session_state.start_date = date.today() - timedelta(days=7)
    if "report_choice" not in st.session_state:
        st.session_state.report_choice = "Last 7 days"


def on_dropdown_change():
    """Callback for when the canned report dropdown changes."""
    today = date.today()
    choice = st.session_state.report_choice

    if choice == "Last 7 days":
        st.session_state.start_date = DateRange.from_days(7)
    elif choice == "Last 30 days":
        st.session_state.start_date = DateRange.from_days(30)
    elif choice == "Last 6 months":
        st.session_state.start_date = DateRange.from_months(6).start
    elif choice == "Last 12 months":
        st.session_state.start_date = DateRange.from_months(12).start

    if choice != "Custom":
        st.session_state.end_date = today


def on_date_change():
    """Callback for when date inputs are modified manually."""
    st.session_state.report_choice = "Custom"


def render_cost_report_tab():
    st.title("AWS Service Cost Report")

    # Render the Control Strip
    with st.container(border=True):
        dropdown, start_date, end_date, top_n_ctrl, run_btn = st.columns(
            [2, 1.5, 1.5, 1, 1]
        )

        with dropdown:
            st.selectbox(
                "Report Period",
                [
                    "Last 7 days",
                    "Last 30 days",
                    "Last 6 months",
                    "Last 12 months",
                    "Custom",
                ],
                key="report_choice",
                on_change=on_dropdown_change,
            )

        with start_date:
            st.date_input("Start Date", key="start_date", on_change=on_date_change)

        with end_date:
            st.date_input("End Date", key="end_date", on_change=on_date_change)

        with top_n_ctrl:
            top_n = st.number_input(
                "Top N", min_value=5, max_value=20, value=10, step=1
            )

        with run_btn:
            # Vertical alignment trick for the button
            st.write("##")
            run_clicked = st.button(
                "Run Report", type="primary", use_container_width=True
            )

    # Fetch data on run
    if run_clicked:
        try:
            dr = DateRange(
                start=st.session_state.start_date, end=st.session_state.end_date
            )

            with st.spinner("Fetching data..."):
                current_profile = os.environ.get("AWS_PROFILE")
                client = create_ce_client(profile_name=current_profile)
                df = generate_cost_report(client, dates=dr, top_n=top_n)

            if not df.empty:
                st.dataframe(
                    df.style.format("{:,.2f}"), use_container_width=True, height=500
                )
            else:
                st.info("No data found.")
        except ValueError as e:
            st.error(f"Configuration Error: {e}")


def render_ui():
    """The main UI layout function."""
    st.set_page_config(layout="wide", page_title="AWS Cost Explorer")

    # 1. Initialize Globals
    initialize_state()

    # 2. Setup Tabs
    tab1, tab2 = st.tabs(["Service Cost Report", "Placeholder "])

    with tab1:
        render_cost_report_tab()

    with tab2:
        st.write("Placeholder")


def start_app():
    """Entry point for [project.scripts] in pyproject.toml."""
    parser = argparse.ArgumentParser(description="AWS Cost Explorer Report")
    parser.add_argument(
        "--profile", type=str, help="AWS CLI profile name", default=None
    )
    args, unknown = parser.parse_known_args()

    # Pass the profile to the Streamlit app via an envvar
    if args.profile:
        os.environ["AWS_PROFILE"] = args.profile

    this_file = str(Path(__file__).resolve())
    sys.argv = ["streamlit", "run", this_file]
    sys.exit(stcli.main())


if __name__ == "__main__":
    render_ui()
