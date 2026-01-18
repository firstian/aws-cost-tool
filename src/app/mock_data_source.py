import os
import time
from collections.abc import Callable, Sequence
from datetime import date, timedelta
from enum import StrEnum
from functools import lru_cache

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from aws_cost_tool.ce_types import CostMetric, DateRange, Granularity


def _generate_mock_ec2_usage(
    service: str, ranges: Sequence[DateRange], tags: Sequence[str]
) -> pd.DataFrame:
    data = []
    groups = [
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
    ]
    for tag in tags:
        data.extend(_generate_mock_usage_data(ranges, service, g, tag) for g in groups)

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


def _generate_mock_ec2_other_usage(
    service: str, ranges: Sequence[DateRange], tags: Sequence[str]
) -> pd.DataFrame:
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
        service,
        {
            "NatGateway-Hours": 10,
            "NatGateway-Bytes": 15,
            "DataTransfer": 5,
            "VpcPeering": 2,
        },
    )
    data.append(vpc_usage)

    # Add some small fake misc items, outside of the well-known categories.
    other_usage = _generate_mock_usage_data(ranges, service, {"Misc": 1})
    data.append(other_usage)

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


def _generate_mock_efs_usage(
    service: str, ranges: Sequence[DateRange], tags: Sequence[str]
) -> pd.DataFrame:
    data = []
    group = {
        "USE1-IADataAccess-Bytes": 1,
        "USE1-IATimedStorage-ByteHrs": 1,
        "USE1-IATimedStorage-ET-ByteHrs": 1,
        "USE1-IATimedStorage-Z-ByteHrs": 1,
        "USE1-IATimedStorage-Z-SmallFiles": 1,
        "USE1-ETDataAccess-Bytes": 5,
        "USE1-TimedStorage-ByteHrs": 5,
        "USE1-TimedStorage-Z-ByteHrs": 5,
        "USE2-TimedStorage-ByteHrs": 5,
    }
    for tag in tags:
        data.append(_generate_mock_usage_data(ranges, service, group, tag))

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


def _generate_mock_rds_usage(
    service: str, ranges: Sequence[DateRange], tags: Sequence[str]
) -> pd.DataFrame:
    data = []
    groups = [
        {
            "USW2-Aurora:BackupUsage": 1,
            "USW2-RDS:ChargedBackupUsage": 1,
            "Aurora:BackupUsage": 1,
            "RDS:ChargedBackupUsage": 1,
        },
        {
            "Aurora:IO-OptimizedStorageUsage": 1,
            "Aurora:StorageIOUsage": 1,
            "Aurora:StorageUsage": 1,
            "RDS:GP2-Storage": 1,
        },
        {
            "DataTransfer-Out-Bytes": 1,
            "USE1-DataTransfer-xAZ-In-Bytes": 1,
        },
        {
            "Aurora:ServerlessV2Usage": 1,
            "InstanceUsage:db.m5.xl": 1,
        },
    ]
    for tag in tags:
        data.extend(_generate_mock_usage_data(ranges, service, g, tag) for g in groups)

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


def _generate_mock_s3_usage(
    service: str, ranges: Sequence[DateRange], tags: Sequence[str]
) -> pd.DataFrame:
    data = []
    groups = [
        {
            "USW2-DataTransfer-Out-Bytes": 1,
            "USW2-USE1-AWS-Out-Bytes": 1,
            "DataTransfer-Out-Bytes": 1,
        },
        {
            "USW2-TimedStorage-ByteHrs": 1,
            "APS3-TimedStorage-ByteHrs": 1,
            "TimedStorage-ByteHrs": 1,
            "TimedStorage-GIR-ByteHrs": 1,
            "TimedStorage-GlacierByteHrs": 1,
            "TimedStorage-INT-AIA-ByteHrs": 1,
        },
        {
            "USW2-Requests-Tier1": 1,
            "USW2-Requests-Tier8": 1,
            "USW2-Tables-Requests-Tier1": 1,
            "APS3-Tables-Requests-Tier1": 1,
            "Requests-Tier1": 1,
            "Requests-Tier8": 1,
            "Tables-Requests-Tier1": 1,
        },
        {
            "Monitoring-Automation-INT": 1,
            "TagStorage-TagHrs": 1,
        },
    ]
    for tag in tags:
        data.extend(_generate_mock_usage_data(ranges, service, g, tag) for g in groups)

    return (
        pd.concat(data)
        .sort_values(by="StartDate", ascending=True)
        .reset_index(drop=True)
    )


class Services(StrEnum):
    _weight: int
    _generator: Callable | None

    EC2 = ("Amazon Elastic Compute Cloud - Compute", 100, _generate_mock_ec2_usage)
    EC2_OTHER = ("EC2 - Other", 30, _generate_mock_ec2_other_usage)
    S3 = ("Amazon Simple Storage Service", 20, _generate_mock_s3_usage)
    RDS = ("Amazon Relational Database Service", 80, _generate_mock_rds_usage)
    LAMBDA = ("Lambda", 5)
    CLOUDWATCH = ("CloudWatch", 10)
    EFS = ("Amazon Elastic File System", 15, _generate_mock_efs_usage)
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
    def generate_usage_data(
        cls,
        service_name: str,
        start: date,
        end: date,
        granularity: str,
        tags: Sequence[str],
    ) -> pd.DataFrame:
        member = cls(service_name)

        if member._generator:
            ranges = _generate_date_ranges(start, end, granularity)
            return member._generator(service_name, ranges, tags)

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
        cost_metric: CostMetric,
        granularity: Granularity,
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
        cost_metric: CostMetric,
        granularity: Granularity,
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
            dates=dates,
            tag_key=tag_key,
            cost_metric=cost_metric,
            granularity=granularity,
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
        ranges.append(DateRange.create(start=current, end=next_step))
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
    date_ranges: Sequence[DateRange],
    service: str,
    usages: dict[str, int],
    tag: str = "",
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
