import pandas as pd
import pytest

from aws_cost_tool.cost_reports import filter_preserve_date_range, generate_cost_report


@pytest.fixture
def aws_cost_df():
    return pd.DataFrame(
        [
            {"StartDate": "2025-01-01", "Service": "A", "Cost": 10.0},
            {"StartDate": "2025-01-01", "Service": "B", "Cost": 5.0},
            {"StartDate": "2025-01-01", "Service": "C", "Cost": 100.0},
            {"StartDate": "2025-01-01", "Service": "D", "Cost": 50.0},
            {"StartDate": "2025-02-01", "Service": "A", "Cost": 20.0},
            {"StartDate": "2025-02-01", "Service": "B", "Cost": 15.0},
            {"StartDate": "2025-02-01", "Service": "C", "Cost": 1.0},
            {"StartDate": "2025-02-01", "Service": "D", "Cost": 50.0},
        ]
    )


def test_generate_cost_report_no_selector(aws_cost_df):
    pivot, total = generate_cost_report(aws_cost_df, "Service")
    # All services should be present, sorted descending by last date.
    assert list(pivot.index) == ["D", "A", "B", "C"]

    # Check Total row
    assert total.loc["Total", "2025-01-01"] == 165.0
    assert total.loc["Total", "2025-02-01"] == 86.0


def test_generate_cost_report_top_n(aws_cost_df):
    # Month 1: A=10, B=5, C=100, D=50 (C, D are Top 2)
    # Month 2: A=20, B=15, C=1, D=50  (A, D are Top 2)
    # That means the pivoted table should see
    # Month 1: A=10, C=100, D=50, Other=5 (same as B)
    # Month 2: A=20, C=1, D=50, Other=15 (same as B)

    # Execute with N=2
    # Top N=1 for Jan is 'C'. Top N=1 for Feb is 'A'.
    # The union should contain both 'A' and 'C', but NOT 'B'.
    pivot, total = generate_cost_report(aws_cost_df, "Service", selector=2)

    # Check Union: 'A', 'C', 'D' should be present, 'B' should be excluded
    assert "A" in pivot.index
    assert "C" in pivot.index
    assert "D" in pivot.index
    assert "Other" in pivot.index
    assert "B" not in pivot.index

    # B is excluded, so it must be in Other
    assert pivot.loc["Other", "2025-01-01"] == 5.0
    assert pivot.loc["Other", "2025-02-01"] == 15.0

    # Check Total row
    assert total.loc["Total", "2025-01-01"] == 165.0
    assert total.loc["Total", "2025-02-01"] == 86.0

    # Check Sorting: Sort by latest column (2025-02-01) descending
    assert list(pivot.index) == ["D", "A", "Other", "C"]


def test_generate_cost_report_row_list(aws_cost_df):
    # Month 1: A=10, B=5, C=100, D=50 (C, D are Top 2)
    # Month 2: A=20, B=15, C=1, D=50  (A, D are Top 2)
    # That means the pivoted table should see
    # Month 1: A=10, C=100, Other=55
    # Month 2: A=20, C=1, Other=65

    # Intentionally add a non-existent column to make sure it still works.
    pivot, total = generate_cost_report(
        aws_cost_df, "Service", selector=["A", "C", "Z"]
    )

    # Check Union: 'A', 'C' should be present, 'B', 'D' should be excluded
    assert "A" in pivot.index
    assert "C" in pivot.index
    assert "Other" in pivot.index
    assert "B" not in pivot.index
    assert "D" not in pivot.index

    # B is excluded, so it must be in Other
    assert pivot.loc["Other", "2025-01-01"] == 55.0
    assert pivot.loc["Other", "2025-02-01"] == 65.0

    # Check Total row
    assert total.loc["Total", "2025-01-01"] == 165.0
    assert total.loc["Total", "2025-02-01"] == 86.0

    # Check Sorting: Sort by latest column (2025-02-01) descending
    assert list(pivot.index) == ["Other", "A", "C"]


def test_generate_cost_report_empty_selector(aws_cost_df):
    with pytest.raises(RuntimeError, match="No rows selected"):
        generate_cost_report(aws_cost_df, "Service", selector=["X", "Y", "Z"])


@pytest.fixture
def sample_df():
    """Create a sample dataframe with multiple dates and categories."""
    return pd.DataFrame(
        {
            "StartDate": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "EndDate": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "Category": ["Software", "Hardware", "Software"],
            "Cost": [100, 200, 300],
            "Extra": ["info1", "info2", "info3"],
        }
    )


def test_filter_removes_rows_but_preserves_dates(sample_df):
    filters = {"Category": "Software"}
    result = filter_preserve_date_range(sample_df, filters)

    # 1. Total rows should equal original unique dates (3)
    assert len(result) == 3

    # 2. Check that 'Hardware' date (Jan 2nd) was filled with 0 cost
    jan_2nd = result[result["StartDate"] == "2025-01-02"].iloc[0]
    assert jan_2nd["Cost"] == 0
    assert jan_2nd["Category"] == "Software"  # Filter value preserved
    assert jan_2nd["Extra"] == ""  # Default empty string


def test_filter_matching_nothing(sample_df):
    # Filter for something that doesn't exist at all
    filters = {"Category": "Cloud"}
    result = filter_preserve_date_range(sample_df, filters)

    # Result should be all filler rows
    assert len(result) == 3
    assert (result["Cost"] == 0).all()
    assert (result["Category"] == "Cloud").all()


def test_multiple_filters(sample_df):
    # Add a column to sample to test multi-filter
    sample_df["Region"] = ["US", "US", "EU"]
    filters = {"Category": "Software", "Region": "US"}

    result = filter_preserve_date_range(sample_df, filters)

    # Original dates: Jan 1, 2, 3.
    # Only Jan 1 matches both. Jan 2 and 3 should be fillers.
    fillers = result[result["Cost"] == 0]
    assert len(fillers) == 2
    assert set(fillers["StartDate"]) == {"2025-01-02", "2025-01-03"}


def test_all_columns_present(sample_df):
    filters = {"Category": "Software"}
    result = filter_preserve_date_range(sample_df, filters)

    # Ensure no columns were lost in the process
    assert set(result.columns) == set(sample_df.columns)


def test_sorting(sample_df):
    # Input a shuffled dataframe
    shuffled_df = sample_df.iloc[[2, 0, 1]]
    filters = {"Category": "Software"}
    result = filter_preserve_date_range(shuffled_df, filters)

    # Dates should be strictly increasing
    dates = result["StartDate"].tolist()
    assert dates == sorted(dates)
