from datetime import timedelta
from enum import StrEnum

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from aws_cost_tool.cost_explorer import DateRange
from aws_cost_tool.cost_reports import cost_report_from_raw_df


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
    SNS = ("SNS", 0)
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


def generate_mock_cost_data(
    dates: DateRange, granularity: str, label: str = ""
) -> pd.DataFrame:
    regions = ["us-east-1", "us-west-2"]

    data = []
    current = dates.start
    while current < dates.end:
        if granularity == "MONTHLY":
            next_step = current + relativedelta(months=1)
        else:
            next_step = current + timedelta(days=1)
            if next_step > dates.end:
                next_step = dates.end

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


def generate_mock_cost_data_with_labels(
    dates: DateRange, granularity: str, labels: list[str]
) -> pd.DataFrame:
    full_df = pd.concat(generate_mock_cost_data(dates, granularity, x) for x in labels)
    return full_df.sort_values(by="StartDate", ascending=True).reset_index(drop=True)


def generate_mock_cost_report(
    dates: DateRange,
    granularity: str = "MONTHLY",
    top_n: int = 10,
    labels: list[str] | None = None,
) -> pd.DataFrame:
    if labels is None:
        labels = [""]
    elif "" not in labels:
        labels = ["", *labels]

    raw_df = generate_mock_cost_data_with_labels(dates, granularity, labels)
    return cost_report_from_raw_df(raw_df, top_n)
