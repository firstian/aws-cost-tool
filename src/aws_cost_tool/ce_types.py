from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

Granularity = Literal["DAILY", "MONTHLY"]
CostMetric = Literal[
    "AmortizedCost",
    "BlendedCost",
    "NetAmortizedCost",
    "NetUnblendedCost",
    "UnblendedCost",
]


@dataclass(frozen=True, kw_only=True)
class DateRange:
    """
    A convenient wrapper for date range that works with Cost Explorer. The end
    date is exclusive, following the AWS boto3 convention.

    The dataclass is frozen to make it hashable.
    """

    start: date
    end: date

    def __post_init__(self):
        if self.start >= self.end:
            raise ValueError("start date must be < end date")

    @classmethod
    def create(cls, start: date | str, end: date | str) -> DateRange:
        """Factory that handles string-to-date conversion."""
        return cls(start=cls._to_date(start), end=cls._to_date(end))

    @classmethod
    def from_days(cls, delta: int, *, end: date | str | None = None) -> DateRange:
        """
        Creates a DateRange by looking back 'delta' number of days from an end date.
        The default end date is today.
        """
        if delta <= 0:
            raise ValueError("delta must be > 0")

        end_date = cls._to_date(end) if end else cls._today()
        start_date = end_date - timedelta(days=delta)
        return cls(start=start_date, end=end_date)

    @classmethod
    def from_months(cls, delta: int, *, end: date | str | None = None) -> DateRange:
        """
        Creates a DateRange by looking back 'delta' number of whole months from
        an end date.
        The default end date is today.
        """
        if delta <= 0 or delta > 12:
            raise ValueError("delta must be > 0")

        end_date = cls._to_date(end) if end else cls._today()

        # Calculate the total months since Year 0
        total_months = (end_date.year * 12 + (end_date.month - 1)) - delta

        # Convert back to year and month
        new_year, month_idx = divmod(total_months, 12)
        start_date = date(new_year, month_idx + 1, 1)

        return cls(start=start_date, end=end_date)

    def to_time_period(self) -> dict[str, str]:
        return {
            "Start": self.start.isoformat(),
            "End": self.end.isoformat(),
        }

    @staticmethod
    def _today() -> date:
        # We need this static method in order to mock out today() for unit testing.
        # The trouble is that date itself is implemented in C, so we can't mock
        # it out and still have the isinstance check in _to_date to work. We also
        # can't just mock out today() itself because of the C implementation.
        return date.today()

    @staticmethod
    def _to_date(value: date | str) -> date:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except ValueError as e:
                raise ValueError(f"Invalid date string: {value}") from e
        raise TypeError(f"Expected date or str, got {type(value).__name__}")
