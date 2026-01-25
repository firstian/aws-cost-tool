from datetime import date
from typing import get_args
from unittest.mock import patch

import pytest

from app.app_state import DateRange, ReportChoice, ReportOptions

# A fixed date for deterministic testing
MOCK_TODAY = date(2025, 5, 20)


class TestReportChoice:
    def test_keys_conform_to_literal(self):
        """Ensure the dictionary keys produced are strictly within ReportOptions."""
        valid_keys = set(get_args(ReportOptions))

        # Test a complex case (LAST_7_DAYS) and the simple case (CUSTOM)
        for choice in [ReportChoice.LAST_7_DAYS, ReportChoice.CUSTOM]:
            settings = choice.settings()
            for key in settings.keys():
                assert key in valid_keys, f"Key '{key}' is not in ReportOptions literal"

    def test_custom_settings_is_minimal(self):
        """Verify CUSTOM only returns the report_choice key."""
        settings = ReportChoice.CUSTOM.settings()

        # Using set comparison to ensure ONLY this key exists
        assert set(settings.keys()) == {"report_choice"}
        assert settings["report_choice"] == ReportChoice.CUSTOM.value

    @patch.object(DateRange, "_today", return_value=MOCK_TODAY)
    def test_last_7_days_logic(self, mock_today):
        """Verifies 7-day range calculation relative to a fixed today."""
        settings = ReportChoice.LAST_7_DAYS.settings()

        # If your DateRange.from_days(7) subtracts 7 days:
        expected_start = date(2025, 5, 13)

        assert settings["report_choice"] == "Last 7 days"
        assert settings["granularity"] == "DAILY"
        assert settings["start_date"] == expected_start
        assert settings["end_date"] == MOCK_TODAY

    @patch.object(DateRange, "_today", return_value=MOCK_TODAY)
    def test_last_6_months_logic(self, mock_today):
        """Verifies 6-month range calculation relative to a fixed today."""
        settings = ReportChoice.LAST_6_MONTHS.settings()

        # Assuming from_months(6) goes back roughly 180 days or to the same day 6 months ago
        # Adjust this expected date based on your DateRange implementation
        expected_start = date(2024, 11, 1)

        assert settings["report_choice"] == "Last 6 months"
        assert settings["granularity"] == "MONTHLY"
        assert settings["start_date"] == expected_start
        assert settings["end_date"] == MOCK_TODAY

    def test_custom_choice_no_dates(self):
        """Verifies CUSTOM does not trigger DateRange or return date keys."""
        # We don't even need to mock here because DateRange shouldn't be called
        settings = ReportChoice.CUSTOM.settings()

        assert settings == {"report_choice": "Custom"}
        assert "start_date" not in settings
        assert "end_date" not in settings

    @pytest.mark.parametrize(
        "choice, expected_granularity",
        [
            (ReportChoice.LAST_7_DAYS, "DAILY"),
            (ReportChoice.LAST_30_DAYS, "DAILY"),
            (ReportChoice.LAST_6_MONTHS, "MONTHLY"),
            (ReportChoice.LAST_12_MONTHS, "MONTHLY"),
        ],
    )
    @patch.object(DateRange, "_today", return_value=MOCK_TODAY)
    def test_granularity_mapping(self, mock_today, choice, expected_granularity):
        """Ensures the correct granularity string is assigned to each choice."""
        settings = choice.settings()
        assert settings["granularity"] == expected_granularity
