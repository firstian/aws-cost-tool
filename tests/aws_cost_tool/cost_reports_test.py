from datetime import date

import pandas as pd

from aws_cost_tool.cost_reports import DateRange, generate_cost_report


def test_generate_cost_report_logic(mocker):
    # 1. Setup Mock Data
    # Month 1: A=10, B=5, C=100 (C is Top 1)
    # Month 2: A=20, B=15, C=1   (A is Top 1)
    raw_data = pd.DataFrame(
        [
            {"StartDate": "2025-01-01", "Service": "A", "Cost": 10.0},
            {"StartDate": "2025-01-01", "Service": "B", "Cost": 5.0},
            {"StartDate": "2025-01-01", "Service": "C", "Cost": 100.0},
            {"StartDate": "2025-02-01", "Service": "A", "Cost": 20.0},
            {"StartDate": "2025-02-01", "Service": "B", "Cost": 15.0},
            {"StartDate": "2025-02-01", "Service": "C", "Cost": 1.0},
        ]
    )

    pivoted_data = pd.DataFrame(
        data={
            "2025-01-01": {"A": 10.0, "B": 5.0, "C": 100.0},
            "2025-02-01": {"A": 20.0, "B": 15.0, "C": 1.0},
        }
    )

    # 2. Mock the external dependencies
    mocker.patch(
        "aws_cost_tool.cost_reports.fetch_service_costs", return_value=raw_data
    )
    mocker.patch("aws_cost_tool.cost_reports.pivot_data", return_value=pivoted_data)

    # Create a dummy DateRange (mocked today for safety)
    mocker.patch.object(DateRange, "_today", return_value=date(2025, 3, 1))
    dr = DateRange.from_months(2)

    # 3. Execute with N=1
    # Top N=1 for Jan is 'C'. Top N=1 for Feb is 'A'.
    # The union should contain both 'A' and 'C', but NOT 'B'.
    result = generate_cost_report(None, dates=dr, top_n=1)

    # 4. Assertions

    # Check Union: 'A' and 'C' should be present, 'B' should be excluded
    assert "A" in result.index
    assert "C" in result.index
    assert "B" not in result.index

    # Check Total row: Should be sum of ALL services in raw_data (including excluded 'B')
    # Jan Total: 10+5+100 = 115.0
    # Feb Total: 20+15+1 = 36.0
    assert result.loc["Total", "2025-01-01"] == 115.0
    assert result.loc["Total", "2025-02-01"] == 36.0

    # Check Sorting: Sort by latest column (2025-02-01) descending
    # In Feb: A (20.0) > C (1.0). Total is always at bottom.
    # Expected index order: ['A', 'C', 'Total']
    assert list(result.index) == ["A", "C", "Total"]
