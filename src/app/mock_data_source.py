import os
import time
from collections.abc import Callable
from datetime import date, timedelta
from enum import StrEnum
from functools import lru_cache

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from aws_cost_tool.cost_explorer import DateRange


def _generate_mock_ec2_usage(
    service: str, start: date, end: date, granularity: str, tags: list[str]
):
    ranges = _generate_date_ranges(start, end, granularity)
    data = []
    for tag in tags:
        box_usage = _generate_mock_usage_data(
            ranges,
            service,
            {
                "USW2-BoxUsage:m5.large": 1,
                "USW2-BoxUsage:m5a.xlarge": 1,
                "BoxUsage:m5.large": 1,
                "BoxUsage:m5a.xlarge": 1,
                "BoxUsage:t3a.xlarge": 1,
                "USE1-HeavyUsage:m5.large": 1,
                "USE1-HeavyUsage:m5a.xlarge": 1,
                "HeavyUsage:m5.large": 1,
                "HeavyUsage:m5a.xlarge": 1,
                "HeavyUsage:t3a.xlarge": 1,
                "USE1-SpotUsage:m5.large": 1,
                "USE1-SpotUsage:m5a.xlarge": 1,
                "USW2-SpotUsage:m7i-flex.large": 1,
                "SpotUsage:m5.large": 2,
                "SpotUsage:m5a.xlarge": 2,
                "SpotUsage:t3a.xlarge": 1,
            },
            tag,
        )
        data_transfer = _generate_mock_usage_data(
            ranges,
            Services.EC2.value,
            {
                "USW2-DataTransfer-In-Byte": 1,
                "USW2-DataTransfer-Out-Byte": 1,
                "DataTransfer-In-Byte": 1,
                "DataTransfer-Out-Byte": 1,
                "USW2-CloudFront-In-Byte": 1,
                "USW2-CloudFront-Out-Byte": 1,
                "USW2-USE1-AWS-In-Bytes": 1,
                "USE1-EUW3-AWS-Out-Bytes": 1,
            },
            tag,
        )
        data.extend([box_usage, data_transfer])

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


def _generate_mock_ec2_other_usage(
    service: str, start: date, end: date, granularity: str, tags: list[str]
) -> pd.DataFrame:
    ranges = _generate_date_ranges(start, end, granularity)
    data = []
    for tag in tags:
        ebs_usage = _generate_mock_usage_data(
            ranges,
            service,
            {
                "EBS:VolumeUsage": 10,
                "EBS:SnapshotUsage": 6,
                "EBS:Throughput": 2,
            },
            tag,
        )
        data.append(ebs_usage)

    vpc_usage = _generate_mock_usage_data(
        ranges,
        "EC2 - Other",
        {
            "NatGateway-Hours": 10,
            "NatGateway-Bytes": 15,
            "DataTransfer": 5,
            "VpcPeering": 2,
        },
    )
    data.append(vpc_usage)

    # Add some small fake misc items, outside of the well-known categories.
    other_usage = _generate_mock_usage_data(
        ranges,
        "EC2 - Other",
        {"Misc": 1},
    )
    data.append(other_usage)

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


class Services(StrEnum):
    _weight: int
    _generator: Callable | None

    EC2 = ("Amazon Elastic Compute Cloud", 100, _generate_mock_ec2_usage)
    EC2_OTHER = ("EC2 - Other", 30, _generate_mock_ec2_other_usage)
    S3 = ("Amazon Simple Storage Service", 20)
    RDS = ("Amazon Relational Database Service", 80)
    LAMBDA = ("Lambda", 5)
    CLOUDWATCH = ("CloudWatch", 10)
    EFS = ("Amazon Elastic File System", 15)
    ROUTE53 = ("Route53", 5)
    SNS = ("SNS", 3)
    SQS = ("SQS", 5)
    KINESIS = ("Kinesis", 10)
    SAGEMAKER = ("SageMaker", 60)
    GUARDDUTY = ("GuardDuty", 10)

    def __new__(cls, tag: str, weight: int, generator: Callable | None = None):
        obj = str.__new__(cls, tag)
        obj._value_ = tag
        obj._weight = weight
        obj._generator = generator
        return obj

    @property
    def weight(self) -> int:
        return self._weight

    @classmethod
    def generate_usage_data(cls, service_name: str, *args, **kwargs) -> pd.DataFrame:
        member = cls(service_name)

        if member._generator:
            return member._generator(service_name, *args, **kwargs)

        raise ValueError(f"Unimplemented for {service_name}")


class Regions(StrEnum):
    US_EAST = "us-east-1"
    US_WEST = "us-west-2"


def add_latency():
    sleep_val = os.environ.get("SLEEP_VAL")
    sleep_sec = 0.25
    if sleep_val is not None:
        if sleep_val == "":
            return

        try:
            sleep_sec = float(sleep_val)
        except Exception:
            pass

    if sleep_sec < 0.0 or sleep_sec > 5.0:
        sleep_sec = 0.25  # out of range, back to default
    time.sleep(sleep_sec)


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
        tags = self.get_tags_for_key(tag_key=tag_key, dates=dates)
        full_df = pd.concat(
            _generate_mock_data(dates.start, dates.end, granularity, x) for x in tags
        )
        add_latency()
        return full_df.sort_values(by="StartDate", ascending=True).reset_index(
            drop=True
        )

    def fetch_service_costs_by_usage(
        self,
        *,
        service: str,
        dates: DateRange,
        tag_key: str = "",
        granularity: str = "MONTHLY",
    ) -> pd.DataFrame:
        add_latency()
        tags = self.get_tags_for_key(tag_key=tag_key, dates=dates)
        data = None

        data = Services.generate_usage_data(
            service, dates.start, dates.end, granularity, tags
        )

        # Fetch all the service costs to leverage the cache, so things look
        # consistent.
        service_cost_df = self.fetch_service_costs(
            dates=dates, tag_key=tag_key, granularity=granularity
        )
        service_cost_df = service_cost_df[service_cost_df["Service"] == service]
        return _normalize_usage_cost(data, service_cost_df)


def _generate_date_ranges(start: date, end: date, granularity: str) -> list[DateRange]:
    ranges = []
    current = start
    while current < end:
        if granularity == "MONTHLY":
            next_step = current + relativedelta(months=1)
        else:
            next_step = current + timedelta(days=1)
            if next_step > end:
                next_step = end
        next_step = min(next_step, end)
        ranges.append(DateRange(start=current, end=next_step))
        current = next_step

    return ranges


@lru_cache
def _generate_mock_data(
    start: date, end: date, granularity: str, tag: str = ""
) -> pd.DataFrame:
    ranges = _generate_date_ranges(start, end, granularity)
    data = []
    for dr in ranges:
        start_date_str = dr.start.strftime("%Y-%m-%d")
        end_date_str = dr.end.strftime("%Y-%m-%d")
        for service in Services:
            for region in Regions:
                cost = np.random.uniform(0.7, 1.3) * service.weight
                data.append(
                    {
                        "StartDate": date.fromisoformat(start_date_str),
                        "EndDate": date.fromisoformat(end_date_str),
                        "Tag": tag,
                        "Service": service,
                        "Region": region,
                        "Cost": round(cost, 2),
                    }
                )

    return pd.DataFrame(data)


def _generate_mock_usage_data(
    date_ranges: list[DateRange], service: str, usages: dict[str, int], tag: str = ""
) -> pd.DataFrame:
    data = []
    for dr in date_ranges:
        start_date_str = dr.start.strftime("%Y-%m-%d")
        end_date_str = dr.end.strftime("%Y-%m-%d")
        for usage_type, weight in usages.items():
            for region in Regions:
                cost = np.random.uniform(0.7, 1.3) * weight
                data.append(
                    {
                        "StartDate": date.fromisoformat(start_date_str),
                        "EndDate": date.fromisoformat(end_date_str),
                        "Tag": tag,
                        "Service": service,
                        "Usage_type": usage_type,
                        "Region": region,
                        "Cost": round(cost, 2),
                    }
                )

    return pd.DataFrame(data)


def _normalize_usage_cost(
    usage_df: pd.DataFrame, service_df: pd.DataFrame
) -> pd.DataFrame:
    group_cols = ["StartDate", "Region"]
    # First lump all the calls of the service together, regardless of tag.
    total_cost = service_df.rename(columns={"Cost": "ActualCost"})
    total_cost = total_cost.groupby(group_cols, as_index=False)["ActualCost"].sum()

    # Normalize the usage within itself, so each (StartDate, Region) add up to 1.0.
    group_sums = usage_df.groupby(group_cols)["Cost"].transform("sum")
    usage_df["NormalizedCost"] = usage_df["Cost"] / group_sums

    # Multiple the normalized cost with the total_cost. We use merge left to
    # broadcast the total_cost into the corresponding row to preserve the shape.
    final_df = usage_df.merge(total_cost, on=group_cols, how="left")
    final_df["ScaledCost"] = final_df["NormalizedCost"] * final_df["ActualCost"]

    # Last step clean up of column names.
    return final_df.drop(columns=["Cost", "ActualCost", "NormalizedCost"]).rename(
        columns={"ScaledCost": "Cost"}
    )
