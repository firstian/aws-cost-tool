import pandas as pd

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
