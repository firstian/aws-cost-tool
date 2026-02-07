import importlib.util

import pandas as pd


def pytest_configure(config):
    # Enable Copy-on-Write for the entire test session
    pd.options.mode.copy_on_write = True

    # Enable 3.0 String inference
    # Note: Requires pyarrow to be installed
    pd.options.future.infer_string = True


# If we detected that the optional packages are not installed (e.g., user only
# want tho bare package without the app), all the app tests are not run.
def pytest_ignore_collect(collection_path, config):
    # Check if the path being collected is the 'app' directory
    if "tests/app" in str(collection_path):
        # If streamlit is not installed, return True to ignore this path
        if importlib.util.find_spec("streamlit") is None:
            return True
    return False
