from typing import Protocol

import pandas as pd

from aws_cost_tool.cost_explorer import DateRange


class CostSource(Protocol):
    def get_tags_for_key(self, *, tag_key: str, dates: DateRange) -> list[str]: ...

    def fetch_service_costs(
        self,
        *,
        dates: DateRange,
        tag_key: str = "",
        granularity: str = "MONTHLY",
    ) -> pd.DataFrame: ...

    def fetch_service_costs_by_usage(
        self,
        *,
        service: str,
        dates: DateRange,
        tag_key: str = "",
        granularity: str = "MONTHLY",
    ) -> pd.DataFrame: ...
