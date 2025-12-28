import pandas as pd

import aws_cost_tool.cost_explorer as ce
from aws_cost_tool.client import create_ce_client


class AWSCostSource:
    def __init__(self, profile: str | None = None):
        self.client = create_ce_client(profile_name=profile)

    def get_tags_for_key(self, **kwargs) -> list[str]:
        return ce.get_tags_for_key(self.client, **kwargs)

    def fetch_service_costs(self, **kwargs) -> pd.DataFrame:
        return ce.fetch_service_costs(self.client, **kwargs)
