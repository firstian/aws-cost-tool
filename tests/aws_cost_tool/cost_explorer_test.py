from datetime import date
from unittest.mock import ANY, Mock

import pandas as pd
import pytest

from aws_cost_tool.cost_explorer import (
    DateRange,
    fetch_cost_by_region,
    fetch_service_costs,
    fetch_service_costs_by_usage,
    get_all_aws_services,
    get_tag_keys,
    get_tags_for_key,
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

    def test_init_end_overrides_delta(self):
        """Test initialization with start date, end date, and delta."""
        start = date(2025, 1, 1)
        end = date(2025, 1, 31)
        dr = DateRange(start=start, end=end, delta=7)
        assert dr.start == start
        assert dr.end == end

    def test_init_with_delta(self):
        """Test initialization with start date and delta."""
        start = date(2025, 1, 1)
        dr = DateRange(start=start, delta=7)
        assert dr.start == start
        assert dr.end == date(2025, 1, 8)

    def test_init_default_delta(self):
        """Test initialization with default delta of 1."""
        start = date(2025, 1, 1)
        dr = DateRange(start=start)
        assert dr.end == date(2025, 1, 2)

    def test_init_negative_delta_defaults_to_one(self):
        """Test that negative delta is converted to 1."""
        start = date(2025, 1, 1)
        dr = DateRange(start=start, delta=-5)
        assert dr.end == date(2025, 1, 2)

    def test_init_zero_delta_defaults_to_one(self):
        """Test that zero delta is converted to 1."""
        start = date(2025, 1, 1)
        dr = DateRange(start=start, delta=0)
        assert dr.end == date(2025, 1, 2)

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
            DateRange(start="invalid-date")

    def test_invalid_type_raises_error(self):
        """Test that invalid type raises TypeError."""
        with pytest.raises(TypeError, match="Expected date or str"):
            DateRange(start=123)  # type: ignore

    def test_to_time_period(self):
        """Test conversion to AWS time period format."""
        dr = DateRange(start="2025-01-01", end="2025-01-31")
        result = dr.to_time_period()
        assert result == {
            "Start": "2025-01-01",
            "End": "2025-01-31",
        }


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


class TestFetchCostByRegion:
    """Tests for fetch_cost_by_region function."""

    def test_single_page_response(self):
        """Test fetching cost entries with single page response."""
        mock_client = Mock()
        mock_client.get_cost_and_usage.return_value = {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2025-01-01", "End": "2025-02-01"},
                    "Groups": [
                        {
                            "Keys": ["Amazon EC2", "us-east-1"],
                            "Metrics": {"UnblendedCost": {"Amount": "100.50"}},
                        },
                        {
                            "Keys": ["Amazon S3", "us-west-2"],
                            "Metrics": {"UnblendedCost": {"Amount": "50.25"}},
                        },
                    ],
                }
            ]
        }

        dates = DateRange(start="2025-01-01", end="2025-02-01")
        filter_expr = {"Tags": {"Key": "env", "Values": ["prod"]}}

        result = fetch_cost_by_region(
            mock_client, filter_expr=filter_expr, dates=dates, label="prod"
        )

        assert len(result) == 2
        assert result.iloc[0]["StartDate"] == dates.start
        assert result.iloc[0]["EndDate"] == dates.end
        assert result.iloc[0]["Service"] == "Amazon EC2"
        assert result.iloc[0]["Region"] == "us-east-1"
        assert result.iloc[0]["Cost"] == 100.50
        assert result.iloc[0]["Label"] == "prod"
        assert result.iloc[1]["Service"] == "Amazon S3"
        assert result.iloc[1]["Cost"] == 50.25

    def test_multi_page_response(self):
        """Test fetching cost entries with pagination."""
        mock_client = Mock()
        mock_client.get_cost_and_usage.side_effect = [
            {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2025-01-01", "End": "2025-02-01"},
                        "Groups": [
                            {
                                "Keys": ["Amazon EC2", "us-east-1"],
                                "Metrics": {"UnblendedCost": {"Amount": "100.00"}},
                            }
                        ],
                    }
                ],
                "NextPageToken": "token123",
            },
            {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2025-01-01", "End": "2025-02-01"},
                        "Groups": [
                            {
                                "Keys": ["Amazon S3", "us-west-2"],
                                "Metrics": {"UnblendedCost": {"Amount": "50.00"}},
                            }
                        ],
                    }
                ]
            },
        ]

        dates = DateRange(start="2025-01-01", end="2025-02-01")
        filter_expr = {"Tags": {"Key": "env", "Values": ["prod"]}}

        result = fetch_cost_by_region(mock_client, filter_expr=filter_expr, dates=dates)

        assert len(result) == 2
        assert mock_client.get_cost_and_usage.call_count == 2

    def test_empty_response(self):
        """Test handling of empty response."""
        mock_client = Mock()
        mock_client.get_cost_and_usage.return_value = {"ResultsByTime": []}

        dates = DateRange(start="2025-01-01", end="2025-02-01")
        filter_expr = {"Tags": {"Key": "env", "Values": ["test"]}}

        result = fetch_cost_by_region(mock_client, filter_expr=filter_expr, dates=dates)

        assert result.empty
        assert list(result.columns) == [
            "StartDate",
            "EndDate",
            "Label",
            "Service",
            "Region",
            "Cost",
        ]

    def test_custom_granularity(self):
        """Test with custom granularity parameter."""
        mock_client = Mock()
        mock_client.get_cost_and_usage.return_value = {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2025-01-01", "End": "2025-01-02"},
                    "Groups": [
                        {
                            "Keys": ["Amazon EC2", "us-east-1"],
                            "Metrics": {"UnblendedCost": {"Amount": "10.00"}},
                        }
                    ],
                }
            ]
        }

        dates = DateRange(start="2025-01-01", end="2025-01-31")
        filter_expr = {}

        fetch_cost_by_region(
            mock_client, filter_expr=filter_expr, dates=dates, granularity="DAILY"
        )

        call_args = mock_client.get_cost_and_usage.call_args[1]
        assert call_args["Granularity"] == "DAILY"


class TestFetchServiceCosts:
    """Tests for fetch_service_costs function."""

    full_response = [
        pd.DataFrame(
            [
                {
                    "StartDate": "2025-01-01",
                    "EndDate": "2025-02-01",
                    "Label": "prod",
                    "Service": "EC2",
                    "Region": "us-east-1",
                    "Cost": 100.0,
                }
            ]
        ),
        pd.DataFrame(
            [
                {
                    "StartDate": "2025-01-01",
                    "EndDate": "2025-02-01",
                    "Label": "staging",
                    "Service": "S3",
                    "Region": "us-west-2",
                    "Cost": 50.0,
                }
            ]
        ),
    ]
    dates = DateRange(start="2025-01-01", end="2025-02-01")

    def test_multiple_tags(self, mocker):
        """Test fetching costs for multiple tags."""
        mock_client = Mock()

        # Mock fetch_cost_by_region to return different data for each tag
        mock_fetch = mocker.patch("aws_cost_tool.cost_explorer.fetch_cost_by_region")
        mock_fetch.side_effect = self.full_response

        # Mock time.sleep to speed up test
        mocker.patch("aws_cost_tool.cost_explorer.time.sleep")

        result = fetch_service_costs(
            mock_client, tag_key="env", tag_values=["prod", "staging"], dates=self.dates
        )

        assert len(result) == 2
        assert mock_fetch.call_count == 2
        assert mock_fetch.call_args_list[0].args == (mock_client,)
        assert mock_fetch.call_args_list[0].kwargs["label"] == "prod"
        assert mock_fetch.call_args_list[1].args == (mock_client,)
        assert mock_fetch.call_args_list[1].kwargs["label"] == "staging"

    def test_empty_tag_list(self, mocker):
        """Test with empty tag list, which lumps everything together."""
        mock_client = Mock()
        mock_fetch = mocker.patch("aws_cost_tool.cost_explorer.fetch_cost_by_region")
        mock_fetch.return_value = self.full_response[0]
        result = fetch_service_costs(mock_client, dates=self.dates)

        assert len(result) == 1
        mock_fetch.assert_called_once_with(
            mock_client, dates=self.dates, granularity="MONTHLY"
        )

    def test_fetch_all_tags_for_key(self, mocker):
        """Test fetching costs for all tags under a key."""
        mock_client = Mock()

        mock_get_tags = mocker.patch("aws_cost_tool.cost_explorer.get_tags_for_key")
        mock_get_tags.return_value = ["prod", "staging"]

        mock_fetch = mocker.patch("aws_cost_tool.cost_explorer.fetch_cost_by_region")
        # Need to use side_effect to return a different thing far each call.
        mock_fetch.side_effect = self.full_response

        result = fetch_service_costs(mock_client, tag_key="env", dates=self.dates)
        mock_get_tags.assert_called_once_with(
            mock_client, tag_key="env", dates=self.dates
        )

        assert len(result) == 2
        assert mock_fetch.call_count == 2


class TestFetchServiceCostsByUsage:
    """Tests for fetch_service_costs_by_usage function."""

    full_response = [
        pd.DataFrame(
            [
                {
                    "StartDate": "2025-01-01",
                    "EndDate": "2025-02-01",
                    "Label": "prod",
                    "Usage_type": "NatGateway-Bytes",
                    "Region": "us-east-1",
                    "Cost": 100.0,
                }
            ]
        ),
        pd.DataFrame(
            [
                {
                    "StartDate": "2025-01-01",
                    "EndDate": "2025-02-01",
                    "Label": "staging",
                    "Usage_type": "NatGateway-Bytes",
                    "Region": "us-west-2",
                    "Cost": 50.0,
                }
            ]
        ),
    ]
    dates = DateRange(start="2025-01-01", end="2025-02-01")

    def test_multiple_tags(self, mocker):
        """Test fetching costs for multiple tags."""
        mock_client = Mock()

        # Mock fetch_cost_by_region to return different data for each tag
        mock_fetch = mocker.patch("aws_cost_tool.cost_explorer.fetch_cost_by_region")
        mock_fetch.side_effect = self.full_response

        # Mock time.sleep to speed up test
        mocker.patch("aws_cost_tool.cost_explorer.time.sleep")

        result = fetch_service_costs_by_usage(
            mock_client,
            service="EC2 - Other",
            tag_key="env",
            tag_values=["prod", "staging"],
            dates=self.dates,
        )

        assert len(result) == 2
        assert result["Service"].unique() == ["EC2 - Other"]
        assert mock_fetch.call_count == 2
        assert mock_fetch.call_args_list[0].args == (mock_client,)
        assert mock_fetch.call_args_list[0].kwargs["group_by"] == "USAGE_TYPE"
        assert mock_fetch.call_args_list[0].kwargs["label"] == "prod"
        filter_expr = mock_fetch.call_args_list[0].kwargs["filter_expr"]
        assert filter_expr["And"][0]["Dimensions"]["Key"] == "SERVICE"
        assert filter_expr["And"][0]["Dimensions"]["Values"] == ["EC2 - Other"]
        assert mock_fetch.call_args_list[1].args == (mock_client,)
        assert mock_fetch.call_args_list[1].kwargs["label"] == "staging"

    def test_empty_tag_list(self, mocker):
        """Test with empty tag list, which lumps everything together."""
        mock_client = Mock()
        mock_fetch = mocker.patch("aws_cost_tool.cost_explorer.fetch_cost_by_region")
        mock_fetch.return_value = self.full_response[0]
        result = fetch_service_costs_by_usage(
            mock_client, service="EC2 - Other", dates=self.dates
        )

        assert len(result) == 1
        mock_fetch.assert_called_once_with(
            mock_client,
            dates=self.dates,
            filter_expr=ANY,
            group_by="USAGE_TYPE",
            granularity="MONTHLY",
        )
        filter_expr = mock_fetch.call_args_list[0].kwargs["filter_expr"]
        assert filter_expr["Dimensions"]["Key"] == "SERVICE"
        assert filter_expr["Dimensions"]["Values"] == ["EC2 - Other"]

    def test_fetch_all_tags_for_key(self, mocker):
        """Test fetching costs for all tags under a key."""
        mock_client = Mock()

        mock_get_tags = mocker.patch("aws_cost_tool.cost_explorer.get_tags_for_key")
        mock_get_tags.return_value = ["prod", "staging"]

        mock_fetch = mocker.patch("aws_cost_tool.cost_explorer.fetch_cost_by_region")
        # Need to use side_effect to return a different thing far each call.
        mock_fetch.side_effect = self.full_response

        result = fetch_service_costs_by_usage(
            mock_client, service="EC2 - Other", tag_key="env", dates=self.dates
        )
        mock_get_tags.assert_called_once_with(
            mock_client, tag_key="env", dates=self.dates
        )

        assert len(result) == 2
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
