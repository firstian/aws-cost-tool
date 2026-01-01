import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sql_tab import get_column_names, get_sql_ready_df

## --- Tests for get_column_names ---


def test_get_column_names_standard():
    # Standard RangeIndex (unnamed) should be ignored
    df = pd.DataFrame({"A": [1], "B": [2]})
    assert get_column_names(df) == ["A", "B"]


def test_get_column_names_named_single_index():
    df = pd.DataFrame({"A": [1]}, index=pd.Index([0], name="Date"))
    assert get_column_names(df) == ["Date", "A"]


def test_get_column_names_multiindex_rows():
    mi = pd.MultiIndex.from_tuples([("Group1", 2023)], names=["Org", "Year"])
    df = pd.DataFrame({"Val": [10]}, index=mi)
    assert get_column_names(df) == ["Org", "Year", "Val"]


def test_get_column_names_multiindex_columns():
    cols = pd.MultiIndex.from_tuples([("Finance", "Cost"), ("Finance", "Tax")])
    df = pd.DataFrame([[100, 20]], columns=cols)
    assert get_column_names(df) == ["Finance_Cost", "Finance_Tax"]


## --- Tests for get_sql_ready_df ---


def test_get_sql_ready_df_no_change():
    # RangeIndex should return the same object (no reset)
    df = pd.DataFrame({"A": [1, 2]})
    result = get_sql_ready_df(df)
    assert result.equals(df)
    assert "index" not in result.columns


def test_get_sql_ready_df_named_single_index():
    # Named single index: logic says reset levels[:-1].
    # For single index, names[:-1] is empty, so it should return df as-is.
    df = pd.DataFrame({"A": [1]}, index=pd.Index([10], name="ID"))
    result = get_sql_ready_df(df)
    assert result.index.name == "ID"
    assert "ID" not in result.columns


def test_get_sql_ready_df_multiindex_partial_reset():
    # mi.names[:-1] should be ['Region']
    mi = pd.MultiIndex.from_tuples([("North", "A")], names=["Region", "ID"])
    df = pd.DataFrame({"Data": [1]}, index=mi)

    result = get_sql_ready_df(df)

    # 'Region' should move to columns, 'ID' stays in index
    assert "Region" in result.columns
    assert result.index.name == "ID"
    assert "ID" not in result.columns
    assert result.iloc[0]["Region"] == "North"


def test_get_sql_ready_df_unnamed_multiindex():
    # If levels are None, named_levels will be empty, returning df as-is
    mi = pd.MultiIndex.from_tuples([("North", "A")], names=[None, None])
    df = pd.DataFrame({"Data": [1]}, index=mi)
    result = get_sql_ready_df(df)
    assert isinstance(result.index, pd.MultiIndex)


## --- Tests for the sql_sandbox tab ---


# This setup script acts as a "Mini App" wrapper for the fragment
FRAGMENT_WRAPPER = """
import streamlit as st
import pandas as pd
from app.sql_tab import render_sql_sandbox

# Set up the data in the test itself.
st.session_state["cost_data"] = {content}
render_sql_sandbox()
st.write("Function finished.")
"""


def sql_at(cost_df_str: str = "", service_df_str: str = ""):
    """Initializes the AppTest with the fragment wrapper."""
    content = []
    if cost_df_str:
        content.append(f'"cost_df": {cost_df_str}')
    if service_df_str:
        content.append(f'"Service": {service_df_str}')

    content_str = ",".join(content)
    script_str = FRAGMENT_WRAPPER.format(content=f"{{{content_str}}}")
    # print(script_str)
    return AppTest.from_string(script_str)


## 1. Test the "Empty State"
def test_sql_sandbox_no_data():
    # The cost_data is empty from the wrapper
    at = sql_at().run()

    assert "No Data" in at.markdown[0].value
    # Ensure editor doesn't even show up
    assert len(at.text_area) == 0


## 2. Test the Schema Table Display
def test_sql_sandbox_table_listing():
    at = sql_at(
        cost_df_str='pd.DataFrame({"Service": ["EC2"], "Cost": [100]})',
        service_df_str='pd.DataFrame({"Service": ["S3"], "Cost": [1.5]})',
    ).run()

    # Check if the schema table rendered the correct row count
    # at.table[0].value is the list of dicts passed to st.table
    schema_data = at.table[0].value
    assert schema_data.iloc[0]["Original Source"] == "cost_df"
    assert schema_data.iloc[0]["Rows"] == 1

    assert schema_data.iloc[1]["Original Source"] == "Service"
    assert schema_data.iloc[1]["Rows"] == 1


## 3. Test Successful SQL Execution
def test_sql_execution_success():
    at = sql_at(
        cost_df_str='pd.DataFrame({"Service": ["EC2"], "Cost": [100]})',
        service_df_str='pd.DataFrame({"amount": [10, 20]})',
    ).run()

    # Input query and click execute
    at.text_area(key="sql_text").set_value(
        "SELECT sum(amount) as total FROM service"
    ).run()
    at.button(key="run_sql").click().run()

    # Check for success message and dataframe presence
    assert "Returned 1 rows" in at.success[0].value
    assert len(at.dataframe) == 1
    # Verify the result content if needed
    assert at.dataframe[0].value.iloc[0]["total"] == 30


## 4. Test Security: Blocking non-SELECT queries
def test_sql_security_block():
    at = sql_at(
        cost_df_str='pd.DataFrame({"Service": ["EC2"], "Cost": [100]})',
        service_df_str='pd.DataFrame({"amount": [10, 20]})',
    ).run()

    # Try a DELETE query
    at.text_area(key="sql_text").set_value("DELETE FROM service").run()
    at.button(key="run_sql").click().run()

    # Verify the error message from your is_query_safe check
    assert "Only SELECT queries are allowed" in at.error[0].value


## 5. Test SQL Syntax Error Handling
def test_sql_syntax_error():
    at = sql_at(
        cost_df_str='pd.DataFrame({"Service": ["EC2"], "Cost": [100]})',
        service_df_str='pd.DataFrame({"amount": [10, 20]})',
    ).run()

    # Input gibberish SQL
    at.text_area(key="sql_text").set_value("SELECT * FROM non_existent_table").run()
    at.button(key="run_sql").click().run()

    # Verify DuckDB error is captured in st.error
    assert "SQL Error" in at.error[0].value
