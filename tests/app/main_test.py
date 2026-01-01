import os

import pytest
from streamlit.testing.v1 import AppTest


@pytest.fixture(scope="function")
def fast_mock_app():
    """
    Fixture to prep the environment and provide a fast-loading AppTest instance.
    """
    # Set the mock flag for the backend
    os.environ["AWS_PROFILE"] = "mock_data"
    os.environ["SLEEP_VAL"] = ""

    # Point to your app location relative to project root
    at = AppTest.from_file("src/app/main.py")
    yield at


def test_main_ui_initial_load(fast_mock_app):
    # at = fast_mock_app.run()
    at = fast_mock_app.run()
    # 1. Verify Page Config / Header
    assert (
        at.title[0].value == "AWS Cost Dashboard"
    )  # Assuming render_header has a title

    # Verify all tabs are present
    expected_tabs = ["Service Cost", "Tagged Cost", "Service Usage", "SQL Sandbox"]
    assert len(at.tabs) == len(expected_tabs)

    for i, label in enumerate(expected_tabs):
        assert at.tabs[i].label == label


@pytest.mark.skip(reason="Segmented control screws this up")
def test_run_button_triggers_report(fast_mock_app):
    # See https://github.com/streamlit/streamlit/issues/11338
    at = fast_mock_app.run()

    # Simulate clicking the button in the control strip
    # We find the button by label
    run_btn = at.button(key="run_btn")  # Adjust label to match your UI
    run_btn.click().run()

    # Verify that the Service Cost tab now shows a result (e.g., a dataframe)
    # We look inside tab index 0
    service_tab = at.tabs[0]
    assert len(service_tab.dataframe) > 0
