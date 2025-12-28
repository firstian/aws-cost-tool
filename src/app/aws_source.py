import pandas as pd

import aws_cost_tool.cost_explorer as ce


class AWSCostSource:
    def get_tags_for_key(self, ce_client, **kwargs) -> list[str]:
        return ce.get_tags_for_key(ce_client, **kwargs)

    def fetch_service_costs(self, ce_client, **kwargs) -> pd.DataFrame:
        return ce.fetch_service_costs(ce_client, **kwargs)
