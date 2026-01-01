from typing import Callable

import pandas as pd
import pytest

from aws_cost_tool.service_base import ServiceBase, slugify_name

# Assuming your function is in this file or imported
# from your_module import slugify_name


def test_slugify_basic():
    """Test standard casing and space replacement."""
    assert slugify_name("Hello World") == "hello-world"
    assert slugify_name("Data Science 101") == "data-science-101"


def test_slugify_unicode_normalization():
    """Test that accents are removed correctly (e.g., é -> e)."""
    assert slugify_name("Café") == "cafe"
    assert slugify_name("München") == "munchen"
    assert slugify_name("Niño") == "nino"


def test_slugify_special_characters():
    """Test that non-alphanumeric symbols are replaced by a single hyphen."""
    assert slugify_name("Cost ($) / Revenue") == "cost-revenue"
    assert slugify_name("User@Email.com!") == "user-email-com"
    assert slugify_name("file_name.v1.0") == "file-name-v1-0"


def test_slugify_multiple_separators():
    """Test that multiple spaces or symbols don't result in double hyphens."""
    assert slugify_name("this   is    spaced") == "this-is-spaced"
    assert slugify_name("Complex---symbols###here") == "complex-symbols-here"


def test_slugify_strip_hyphens():
    """Test that leading and trailing hyphens are removed."""
    assert slugify_name("  -Leading and Trailing-  ") == "leading-and-trailing"
    assert slugify_name("!!!Surrounded!!!") == "surrounded"


def test_slugify_empty_or_only_symbols():
    """Test behavior when input results in no valid characters."""
    assert slugify_name("!!!") == ""
    assert slugify_name("   ") == ""


@pytest.mark.parametrize(
    "input_str, expected",
    [
        ("Simple Test", "simple-test"),
        ("Upper CASE", "upper-case"),
        ("numbers 123", "numbers-123"),
        ("résumé", "resume"),
    ],
)
def test_slugify_parameterized(input_str, expected):
    """A clean way to test multiple simple variations at once."""
    assert slugify_name(input_str) == expected


# Mocking Extractor type for clarity (assuming it's a Callable returning a DF)
Extractor = Callable[[pd.DataFrame], pd.DataFrame]

## --- Test Setup ---


class MockService(ServiceBase):
    @property
    def name(self):
        return "Mock Service"

    @property
    def shortname(self):
        return "mock"

    def categorize_usage(self, df):
        return df  # Not used in this specific test


@pytest.fixture
def sample_df():
    """Create a basic cost dataframe with 4 rows."""
    return pd.DataFrame(
        {
            "Cost": [10.0, 20.0, 30.0, 40.0],
            "UsageType": [
                "Compute-Small",
                "Compute-Large",
                "Storage-S3",
                "Unknown-Misc",
            ],
        },
        index=[0, 1, 2, 3],
    )


## --- Test Cases ---


def test_categorize_usage_costs_empty():
    service = MockService()
    result = service.categorize_usage_costs(pd.DataFrame(), extractors={})
    assert result.empty
    assert isinstance(result, pd.DataFrame)


def test_categorize_usage_costs_partitioning(sample_df):
    service = MockService()

    # Define extractors that split the data
    extractors = {
        "Compute": lambda df: df[df["UsageType"].str.contains("Compute")],
        "Storage": lambda df: df[df["UsageType"].str.contains("Storage")],
    }

    result = service.categorize_usage_costs(sample_df, extractors=extractors)

    # Verify MultiIndex Level 0 (Category)
    assert set(result.index.get_level_values("Category")) == {
        "Compute",
        "Storage",
        "Other",
    }

    # Verify specific counts
    assert len(result.loc["Compute"]) == 2
    assert len(result.loc["Storage"]) == 1
    assert len(result.loc["Other"]) == 1

    # Verify "Other" logic
    other_row = result.loc["Other"]
    assert other_row["UsageType"].iloc[0] == "Unknown-Misc"
    assert other_row["Subtype"].iloc[0] == "Other"


def test_categorize_usage_costs_index_integrity(sample_df):
    service = MockService()
    extractors = {"All": lambda df: df}  # Grab everything

    result = service.categorize_usage_costs(sample_df, extractors=extractors)

    # Check that original indices (0, 1, 2, 3) are preserved in level 1
    # Level 0 is 'Category', Level 1 is the original index
    assert list(result.index.get_level_values(1)) == [0, 1, 2, 3]
    assert result.index.names[0] == "Category"
    assert (
        result.index.names[1] is None
    )  # Matches your requirement to leave original index unnamed


def test_categorize_usage_costs_no_others(sample_df):
    """Test that 'Other' is not added if extractors cover the entire union."""
    service = MockService()
    # Extractor that takes everything
    extractors = {"Everything": lambda df: df}

    result = service.categorize_usage_costs(sample_df, extractors=extractors)

    assert "Other" not in result.index.get_level_values("Category")
    assert len(result) == len(sample_df)


def test_categorize_usage_costs_overlapping_extractors(sample_df):
    """
    Test how concat handles rows if extractors have overlapping logic.
    Note: pd.concat will duplicate rows if they appear in multiple groups.
    """
    service = MockService()
    extractors = {"GroupA": lambda df: df.iloc[[0]], "GroupB": lambda df: df.iloc[[0]]}

    result = service.categorize_usage_costs(sample_df, extractors=extractors)

    # Row 0 appears twice, plus the 'Other' group (rows 1, 2, 3)
    assert len(result) == 5
    assert len(result.loc["Other"]) == 3
