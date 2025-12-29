from datetime import date, timedelta
from enum import StrEnum
from functools import lru_cache

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from aws_cost_tool.cost_explorer import DateRange


class Services(StrEnum):
    _weight: int

    EC2 = ("EC2", 100)
    EC2_OTHER = ("EC2 - Other", 30)
    S3 = ("S3", 20)
    RDS = ("RDS", 80)
    LAMBDA = ("Lambda", 5)
    CLOUDWATCH = ("CloudWatch", 10)
    DYNAMODB = ("DynamoDB", 15)
    ROUTE53 = ("Route53", 5)
    SNS = ("SNS", 3)
    SQS = ("SQS", 5)
    KINESIS = ("Kinesis", 10)
    SAGEMAKER = ("SageMaker", 60)
    GUARDDUTY = ("GuardDuty", 10)

    def __new__(cls, label: str, weight: int):
        obj = str.__new__(cls, label)
        obj._value_ = label
        obj._weight = weight
        return obj

    @property
    def weight(self) -> int:
        return self._weight


class MockCostSource:
    def get_tags_for_key(self, *, tag_key: str, dates: DateRange) -> list[str]:
        return [""] + [f"{tag_key}:project{i}" for i in range(3)]

    def fetch_service_costs(
        self,
        *,
        dates: DateRange,
        tag_key: str = "",
        granularity: str = "MONTHLY",
    ) -> pd.DataFrame:
        labels = self.get_tags_for_key(tag_key=tag_key, dates=dates)
        full_df = pd.concat(
            _generate_mock_data(dates.start, dates.end, granularity, x) for x in labels
        )
        return full_df.sort_values(by="StartDate", ascending=True).reset_index(
            drop=True
        )


@lru_cache
def _generate_mock_data(
    start: date, end: date, granularity: str, label: str = ""
) -> pd.DataFrame:
    regions = ["us-east-1", "us-west-2"]

    data = []
    current = start
    while current < end:
        if granularity == "MONTHLY":
            next_step = current + relativedelta(months=1)
        else:
            next_step = current + timedelta(days=1)
            if next_step > end:
                next_step = end

        for service in Services:
            for region in regions:
                cost = np.random.uniform(0.7, 1.3) * service.weight
                data.append(
                    {
                        "StartDate": current.strftime("%Y-%m-%d"),
                        "EndDate": next_step.strftime("%Y-%m-%d"),
                        "Label": label,
                        "Service": service,
                        "Region": region,
                        "Cost": round(cost, 2),
                    }
                )

        current = next_step

    return pd.DataFrame(data)
