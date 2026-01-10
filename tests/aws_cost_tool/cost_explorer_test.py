from datetime import date, timedelta
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from aws_cost_tool.cost_explorer import (
    DateRange,
    fetch_active_regions,
    fetch_service_costs,
    fetch_service_costs_by_usage,
    get_all_aws_services,
    get_tag_keys,
    get_tags_for_key,
    paginate_ce,
    pivot_data,
    summarize_by_columns,
)


class TestDateRange:
    """Tests for DateRange class."""

    def test_init_with_dates(self):
        """Test initialization with date objects."""
        start = date(2025, 1, 1)
        end = date(2025, 1, 31)
        dr = DateRange(start=start, end=end)
        assert dr.start == start
        assert dr.end == end

    def test_init_with_strings(self):
        """Test initialization with ISO string dates."""
        dr = DateRange(start="2025-01-01", end="2025-01-31")
        assert dr.start == date(2025, 1, 1)
        assert dr.end == date(2025, 1, 31)

    def test_init_start_equals_end_raises_error(self):
        """Test that start >= end raises ValueError."""
        start = date(2025, 1, 1)
        with pytest.raises(ValueError, match="start date must be < end date"):
            DateRange(start=start, end=start)

    def test_init_start_after_end_raises_error(self):
        """Test that start > end raises ValueError."""
        with pytest.raises(ValueError, match="start date must be < end date"):
            DateRange(start="2025-01-31", end="2025-01-01")

    def test_invalid_date_string_raises_error(self):
        """Test that invalid date string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date string"):
            DateRange(start="invalid-date", end="2025-01-01")

    def test_invalid_type_raises_error(self):
        """Test that invalid type raises TypeError."""
        with pytest.raises(TypeError, match="Expected date or str"):
            DateRange(start=123, end="20250101")  # type: ignore

    def test_to_time_period(self):
        """Test conversion to AWS time period format."""
        dr = DateRange(start="2025-01-01", end="2025-01-31")
        result = dr.to_time_period()
        assert result == {
            "Start": "2025-01-01",
            "End": "2025-01-31",
        }

    def test_from_days_explicit_end(self):
        # 10 days back from Nov 10
        dr = DateRange.from_days(10, end="2025-11-10")
        assert dr.start == date(2025, 10, 31)
        assert dr.end == date(2025, 11, 10)

    def test_from_days_default_today(self, mocker):
        end_date = date(2025, 1, 5)
        mocker.patch.object(DateRange, "_today", return_value=end_date)

        dr = DateRange.from_days(9)
        assert dr.end == end_date
        assert dr.start == end_date - timedelta(days=9)

    def test_from_days_negative_delta(self):
        # Invalid: zero or negative
        with pytest.raises(ValueError, match="delta must be > 0"):
            DateRange.from_days(0)

    def test_from_months_explicit_end(self):
        dr = DateRange.from_months(3, end="2025-11-05")
        assert dr.start == date(2025, 8, 1)
        assert dr.end == date(2025, 11, 5)

    def test_from_months_year_boundary(self):
        # Jan 2025 back 2 months -> Nov 2024
        dr = DateRange.from_months(2, end="2025-01-15")
        assert dr.start == date(2024, 11, 1)

    def test_from_months_default_today(self, mocker):
        end_date = date(2025, 2, 5)
        mocker.patch.object(DateRange, "_today", return_value=end_date)

        dr = DateRange.from_months(3)
        assert dr.end == end_date
        assert dr.start == date(2024, 11, 1)

    def test_from_months_negative_delta(self):
        with pytest.raises(ValueError, match="delta must be > 0"):
            DateRange.from_months(0)


class TestGetAwsServices:
    def test_get_all_aws_services_success(self):
        """Test normal path and return sorted service names"""
        mock_ce = Mock()
        mock_response = {
            "DimensionValues": [
                {"Value": "Amazon Simple Storage Service"},
                {"Value": "Tax"},
                {"Value": "Amazon Elastic Compute Cloud - Compute"},
            ]
        }

        mock_ce.get_dimension_values.return_value = mock_response

        dates = DateRange(start="2025-01-01", end="2025-02-01")
        result = get_all_aws_services(mock_ce, dates=dates)

        assert result == [
            "Amazon Elastic Compute Cloud - Compute",
            "Amazon Simple Storage Service",
            "Tax",
        ]

        # Verify that get_dimension_values was called with the correct parameters
        mock_ce.get_dimension_values.assert_called_once_with(
            TimePeriod=dates.to_time_period(),
            Dimension="SERVICE",
            Context="COST_AND_USAGE",
        )

    def test_get_all_aws_services_error(self):
        """Boto3 ClientError handling."""
        mock_ce = Mock()

        # Simulate an AWS Exception
        mock_ce.get_dimension_values.side_effect = Exception("AWS Access Denied")

        dates = DateRange(start="2025-01-01", end="2025-02-01")
        with pytest.raises(Exception) as excinfo:
            get_all_aws_services(mock_ce, dates=dates)

        assert "AWS Access Denied" in str(excinfo.value)


class TestGetTagKeys:
    """Tests for get_tag_keys function."""

    def test_successful_fetch(self):
        """Test successful tag keys retrieval."""
        mock_client = Mock()
        mock_client.get_tags.return_value = {"Tags": ["env", "project", "team"]}

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = get_tag_keys(mock_client, dates=dates)

        assert result == ["env", "project", "team"]
        mock_client.get_tags.assert_called_once_with(
            TimePeriod={"Start": "2025-01-01", "End": "2025-01-31"}
        )

    def test_empty_tags(self):
        """Test when no tags are returned."""
        mock_client = Mock()
        mock_client.get_tags.return_value = {}

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = get_tag_keys(mock_client, dates=dates)

        assert result == []

    def test_exception_handling(self, capsys):
        """Test exception handling returns empty list."""
        mock_client = Mock()
        mock_client.get_tags.side_effect = Exception("API Error")

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = get_tag_keys(mock_client, dates=dates)

        assert result == []
        captured = capsys.readouterr()
        assert "Error fetching tag keys: API Error" in captured.out


class TestGetTagsForKey:
    """Tests for get_tags_for_key function."""

    def test_successful_fetch(self):
        """Test successful tag values retrieval."""
        mock_client = Mock()
        mock_client.get_tags.return_value = {
            "Tags": ["prod", "staging", "aws:cloudformation:stack-name"]
        }

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = get_tags_for_key(mock_client, tag_key="env", dates=dates)

        assert result == ["prod", "staging"]
        mock_client.get_tags.assert_called_once_with(
            TimePeriod={"Start": "2025-01-01", "End": "2025-01-31"}, TagKey="env"
        )

    def test_filters_aws_tags(self):
        """Test that aws: prefixed tags are filtered out."""
        mock_client = Mock()
        mock_client.get_tags.return_value = {
            "Tags": ["aws:tag1", "aws:tag2", "custom-tag"]
        }

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = get_tags_for_key(mock_client, tag_key="test", dates=dates)

        assert result == ["custom-tag"]

    def test_exception_handling(self, capsys):
        """Test exception handling returns empty list."""
        mock_client = Mock()
        mock_client.get_tags.side_effect = Exception("API Error")

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = get_tags_for_key(mock_client, tag_key="env", dates=dates)

        assert result == []
        captured = capsys.readouterr()
        assert "Error fetching tag keys: API Error" in captured.out


class TestPaginateCe:
    """Tests for paginate_ce function"""

    def test_paginate_ce_single_page(self):
        mock_client = Mock()
        mock_client.get_cost_and_usage.return_value = {
            "ResultsByTime": [],
        }
        params = {"TimePeriod": {"Start": "2025-01-01", "End": "2025-01-31"}}

        results = list(paginate_ce(mock_client, params))

        assert len(results) == 1
        mock_client.get_cost_and_usage.assert_called_once_with(**params)

    def test_paginate_ce_multiple_pages(self):
        mock_client = Mock()
        mock_client.get_cost_and_usage.side_effect = [
            {"ResultsByTime": [], "NextPageToken": "token1"},
            {"ResultsByTime": [], "NextPageToken": "token2"},
            {"ResultsByTime": []},
        ]
        params = {"TimePeriod": {"Start": "2025-01-01", "End": "2025-01-31"}}

        results = list(paginate_ce(mock_client, params))

        assert len(results) == 3
        assert mock_client.get_cost_and_usage.call_count == 3


class TestFetchActiveRegions:
    """Tests for fetch_active_regions function"""

    def test_fetch_active_regions(self):
        mock_client = Mock()
        mock_client.get_cost_and_usage.return_value = {
            "ResultsByTime": [
                {
                    "Groups": [
                        {
                            "Keys": ["us-east-1"],
                            "Metrics": {"UnblendedCost": {"Amount": "100.50"}},
                        },
                        {
                            "Keys": ["us-west-2"],
                            "Metrics": {"UnblendedCost": {"Amount": "0.005"}},
                        },
                        {
                            "Keys": ["eu-west-1"],
                            "Metrics": {"UnblendedCost": {"Amount": "50.25"}},
                        },
                    ]
                }
            ]
        }
        dr = DateRange(start=date(2025, 1, 1), end=date(2025, 1, 31))
        result = fetch_active_regions(mock_client, dr, "MONTHLY", min_cost=0.01)

        assert set(result) == {"us-east-1", "eu-west-1"}


class TestFetchServiceCost:
    """Tests for fetch_service_cost function"""

    @patch("aws_cost_tool.cost_explorer.paginate_ce")
    def test_fetch_service_cost_no_tag(self, mock_paginate):
        mock_client = Mock()
        mock_paginate.return_value = [
            {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2025-01-01", "End": "2025-01-31"},
                        "Groups": [
                            {
                                "Keys": ["Amazon EC2", "us-east-1"],
                                "Metrics": {"UnblendedCost": {"Amount": "100.50"}},
                            },
                            {
                                "Keys": ["Amazon S3", "us-west-2"],
                                "Metrics": {"UnblendedCost": {"Amount": "25.75"}},
                            },
                        ],
                    }
                ]
            }
        ]
        dates = DateRange(start="2025-01-01", end="2025-01-31")

        result = fetch_service_costs(mock_client, dates=dates)

        assert len(result) == 2
        assert list(result.columns) == [
            "StartDate",
            "EndDate",
            "Service",
            "Region",
            "Cost",
        ]
        assert result["Cost"].sum() == pytest.approx(126.25)

    @patch("aws_cost_tool.cost_explorer.fetch_active_regions")
    @patch("aws_cost_tool.cost_explorer._fetch_group_by_cost")
    @patch("time.sleep")
    def test_fetch_service_cost_with_tag(self, mock_sleep, mock_fetch, mock_regions):
        mock_client = Mock()
        mock_regions.return_value = ["us-east-1", "us-west-2"]

        df1 = pd.DataFrame(
            {
                "StartDate": [date(2025, 1, 1)],
                "EndDate": [date(2025, 1, 31)],
                "Service": ["Amazon EC2"],
                "Environment": ["Environment$prod"],
                "Cost": [100.0],
            }
        )
        df2 = pd.DataFrame(
            {
                "StartDate": [date(2025, 1, 1)],
                "EndDate": [date(2025, 1, 31)],
                "Service": ["Amazon S3"],
                "Environment": ["Environment$dev"],
                "Cost": [50.0],
            }
        )
        mock_fetch.side_effect = [df1, df2]

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = fetch_service_costs(mock_client, dates=dates, tag_key="Environment")

        assert len(result) == 2
        assert "Region" in result.columns
        assert mock_sleep.call_count == 2


class TestFetchServiceCostsByUsage:
    """Tests for fetch_service_costs_by_usage function"""

    @patch("aws_cost_tool.cost_explorer.paginate_ce")
    def test_fetch_service_costs_by_usage_no_tag(self, mock_paginate):
        mock_client = Mock()
        mock_paginate.return_value = [
            {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2025-01-01", "End": "2025-01-31"},
                        "Groups": [
                            {
                                "Keys": ["BoxUsage:t2.micro", "us-east-1"],
                                "Metrics": {"UnblendedCost": {"Amount": "10.50"}},
                            },
                        ],
                    }
                ]
            }
        ]
        dates = DateRange(start="2025-01-01", end="2025-01-31")

        result = fetch_service_costs_by_usage(
            mock_client, service="Amazon EC2", dates=dates
        )

        assert len(result) == 1
        assert "Usage_type" in result.columns

    @patch("aws_cost_tool.cost_explorer.fetch_active_regions")
    @patch("aws_cost_tool.cost_explorer._fetch_group_by_cost")
    @patch("time.sleep")
    def test_fetch_service_costs_by_usage_with_tag(
        self, mock_sleep, mock_fetch, mock_regions
    ):
        mock_client = Mock()
        mock_regions.return_value = ["us-east-1", "us-west-2"]

        df1 = pd.DataFrame(
            {
                "StartDate": [date(2025, 1, 1)],
                "EndDate": [date(2025, 1, 31)],
                "Usage_type": ["BoxUsage:t2.micro"],
                "Environment": ["Environment$prod"],
                "Cost": [50.0],
            }
        )
        df2 = pd.DataFrame(
            {
                "StartDate": [date(2025, 1, 1)],
                "EndDate": [date(2025, 1, 31)],
                "Usage_type": ["BoxUsage:t2.small"],
                "Environment": ["Environment$dev"],
                "Cost": [25.0],
            }
        )
        mock_fetch.side_effect = [df1, df2]

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        result = fetch_service_costs_by_usage(
            mock_client, service="Amazon EC2", dates=dates, tag_key="Environment"
        )

        assert len(result) == 2
        assert "Region" in result.columns
        assert "Tag" in result.columns
        assert "Usage_type" in result.columns
        assert mock_sleep.call_count == 2
        # Verify the tag prefix was stripped
        assert all(not tag.startswith("Environment$") for tag in result["Tag"])
        # Verify service filter was applied to both regions
        assert mock_fetch.call_count == 2


class TestSummarizeByColumns:
    """Tests for summarize_by_columns function."""

    def test_basic_aggregation(self):
        """Test basic cost aggregation by columns."""
        df = pd.DataFrame(
            [
                {"Service": "EC2", "Region": "us-east-1", "Cost": 100.0},
                {"Service": "EC2", "Region": "us-east-1", "Cost": 50.0},
                {"Service": "S3", "Region": "us-west-2", "Cost": 25.0},
            ]
        )

        result = summarize_by_columns(df, ["Service", "Region"], threshold=0)

        assert len(result) == 2
        ec2_cost = result[result["Service"] == "EC2"]["Cost"].values[0]
        assert ec2_cost == 150.0

    def test_threshold_filtering(self):
        """Test filtering by cost threshold."""
        df = pd.DataFrame(
            [
                {"Service": "EC2", "Region": "us-east-1", "Cost": 100.0},
                {"Service": "S3", "Region": "us-west-2", "Cost": 0.0005},
            ]
        )

        result = summarize_by_columns(df, ["Service"], threshold=0.001)

        assert len(result) == 1
        assert result.iloc[0]["Service"] == "EC2"

    def test_none_threshold(self):
        """Test with None threshold (no filtering)."""
        df = pd.DataFrame(
            [
                {"Service": "EC2", "Cost": 100.0},
                {"Service": "S3", "Cost": 0.0001},
            ]
        )

        result = summarize_by_columns(df, ["Service"], threshold=None)  # type: ignore

        assert len(result) == 2


class TestPivotData:
    """Tests for pivot_data function."""

    def test_basic_pivot(self):
        """Test basic pivot table creation."""
        df = pd.DataFrame(
            [
                {"StartDate": "2025-01", "Service": "EC2", "Cost": 100.0},
                {"StartDate": "2025-01", "Service": "S3", "Cost": 50.0},
                {"StartDate": "2025-02", "Service": "EC2", "Cost": 120.0},
            ]
        )

        result = pivot_data(df, row_label="StartDate", col_label="Service", threshold=0)

        assert result.loc["2025-01", "EC2"] == 100.0
        assert result.loc["2025-01", "S3"] == 50.0
        assert result.loc["2025-02", "EC2"] == 120.0

    def test_fillna_with_zero(self):
        """Test that missing values are filled with 0."""
        df = pd.DataFrame(
            [
                {"StartDate": "2025-01", "Service": "EC2", "Cost": 100.0},
                {"StartDate": "2025-02", "Service": "S3", "Cost": 50.0},
            ]
        )

        result = pivot_data(df, row_label="StartDate", col_label="Service", threshold=0)

        assert result.loc["2025-01", "S3"] == 0.0
        assert result.loc["2025-02", "EC2"] == 0.0

    def test_with_threshold(self):
        """Test pivot with cost threshold."""
        df = pd.DataFrame(
            [
                {"StartDate": "2025-01", "Service": "EC2", "Cost": 100.0},
                {"StartDate": "2025-01", "Service": "S3", "Cost": 0.0005},
            ]
        )

        result = pivot_data(
            df, row_label="StartDate", col_label="Service", threshold=0.001
        )

        assert "EC2" in result.columns
        assert "S3" not in result.columns
        assert "S3" not in result.columns
        result = pivot_data(
            df, row_label="StartDate", col_label="Service", threshold=0.001
        )

        assert "EC2" in result.columns
        assert "S3" not in result.columns
