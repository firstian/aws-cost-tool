from enum import StrEnum
from typing import Any, Literal

from aws_cost_tool.cost_explorer import DateRange

ReportOptions = Literal["report_choice", "start_date", "end_date", "granularity"]
ReportSettings = dict[ReportOptions, Any]


class ReportChoice(StrEnum):
    """Enum to be used with the dropdown"""

    LAST_7_DAYS = "Last 7 days"
    LAST_30_DAYS = "Last 30 days"
    LAST_3_MONTHS = "Last 3 months"
    LAST_6_MONTHS = "Last 6 months"
    LAST_12_MONTHS = "Last 12 months"
    CUSTOM = "Custom"

    def settings(self) -> ReportSettings:
        settings: ReportSettings = {"report_choice": self.value}
        if self == ReportChoice.CUSTOM:
            return settings
        match self:
            case ReportChoice.LAST_7_DAYS:
                dr = DateRange.from_days(7)
                granularity = "DAILY"
            case ReportChoice.LAST_30_DAYS:
                dr = DateRange.from_days(30)
                granularity = "DAILY"
            case ReportChoice.LAST_3_MONTHS:
                dr = DateRange.from_months(3)
                granularity = "MONTHLY"
            case ReportChoice.LAST_6_MONTHS:
                dr = DateRange.from_months(6)
                granularity = "MONTHLY"
            case ReportChoice.LAST_12_MONTHS:
                dr = DateRange.from_months(12)
                granularity = "MONTHLY"
            case _:
                raise ValueError(f"Unknown choice {self}")

        settings["start_date"] = dr.start
        settings["end_date"] = dr.end
        settings["granularity"] = granularity

        return settings
