from datetime import date

import pandas as pd

from aws_cost_tool.cost_reports import generate_cost_report


def test_generate_cost_report_logic(mocker):
    # Setup Mock Data
    # Month 1: A=10, B=5, C=100, D=50 (C, D are Top 2)
    # Month 2: A=20, B=15, C=1, D=50  (A, D are Top 2)
    # That means the pivoted table should see
    # Month 1: A=10, C=100, D=50, Others=5 (same as B)
    # Month 2: A=20, C=1, D=50, Others=15 (same as B)
    raw_data = pd.DataFrame(
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

    # Execute with N=2
    # Top N=1 for Jan is 'C'. Top N=1 for Feb is 'A'.
    # The union should contain both 'A' and 'C', but NOT 'B'.
    pivot, total = generate_cost_report(raw_data, "Service", top_n=2)

    # 4. Assertions

    # Check Union: 'A', 'C', 'D' should be present, 'B' should be excluded
    assert "A" in pivot.index
    assert "C" in pivot.index
    assert "D" in pivot.index
    assert "Others" in pivot.index
    assert "B" not in pivot.index

    # B is excluded, so it must be in Others
    assert pivot.loc["Others", "2025-01-01"] == 5.0
    assert pivot.loc["Others", "2025-02-01"] == 15.0

    # Check Total row: Should be sum of ALL services in raw_data (including excluded 'B')
    assert total.loc["Total", "2025-01-01"] == 165.0
    assert total.loc["Total", "2025-02-01"] == 86.0

    # Check Sorting: Sort by latest column (2025-02-01) descending
    # In Feb: A (20.0) > C (1.0). Total is always at bottom.
    # Expected index order: ['A', 'C', 'Total']
    assert list(pivot.index) == ["D", "A", "Others", "C"]
