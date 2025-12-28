from typing import Protocol

import pandas as pd

from aws_cost_tool.cost_explorer import DateRange


class CostSource(Protocol):
    def get_tags_for_key(
        self, ce_client, *, tag_key: str, dates: DateRange
    ) -> list[str]: ...

    def fetch_service_costs(
        self,
        ce_client,
        *,
        dates: DateRange,
        tag_key: str = "",
        granularity: str = "MONTHLY",
    ) -> pd.DataFrame: ...
