import re
import unicodedata
from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import reduce

import pandas as pd

type Extractor = Callable[[pd.DataFrame], pd.DataFrame]


def slugify_name(name: str) -> str:
    """
    Normalizes a string to be filesystem-friendly:
    lowercase, no spaces, no special characters.
    """
    # Convert to lowercase and normalize unicode (e.g., convert 'é' to 'e')
    text = name.lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

    # Replace any non-alphanumeric characters with a hyphen
    # [^a-z0-9]+ matches any sequence of characters that AREN'T a-z or 0-9
    text = re.sub(r"[^a-z0-9]+", "-", text)

    # Remove leading/trailing hyphens
    return text.strip("-")


class ServiceBase(ABC):
    """Base class for all usage cost processing plugins."""

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The display name of the service used by Cost Explorer. It is also used
        to as a filter in the API call, so it has to match exactly what Cost
        Explorer returns.
        """
        ...

    @property
    @abstractmethod
    def shortname(self) -> str:
        """The display name of the service"""
        ...

    @property
    def file_prefix(self) -> str:
        """
        Normalizes a string to be filesystem-friendly:
        lowercase, no spaces, no special characters.
        """
        return slugify_name(self.shortname)

    @abstractmethod
    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        ...

    def categorize_usage_costs(
        self,
        df: pd.DataFrame,
        *,
        extractors: dict[str, Extractor],
        min_cost: float = 0.01,
    ) -> pd.DataFrame:
        """
        A utility that creates a categorized version of usage cost DataFrame,
        given a table of extractors. The key of the extractors are used in the
        level 0 index of the returned DataFrame.

        Extractors should always filter and then make a copy of the input DataFrame.
        """
        if df.empty:
            return pd.DataFrame()

        # First filter out rows with cost below min_cost.
        df = df[df["Cost"] >= min_cost]
        groups = {key: func(df) for key, func in extractors.items()}
        indices = [df.index for df in groups.values()]
        union_index = reduce(lambda x, y: x.union(y), indices)
        other_index = df.index.difference(union_index)
        if not other_index.empty:
            other_df = df.loc[other_index]
            other_df["Subtype"] = "Other"
            groups["Other"] = other_df
        final_df = pd.concat(groups, names=["Category"])
        # Flatten the multi-index by turning Category into a column, and start
        # the numerical index from a clean slate.
        final_df = final_df.reset_index(0).reset_index(drop=True)
        return final_df

    @staticmethod
    def strip_region_prefix_from_usage(df: pd.DataFrame) -> None:
        """
        Many Usage_type returned has a region prefix. This function strips it to
        aid aggregation.
        """
        # Detect region prefix: 2-3 letters, follows by digit and a hyphen.
        region_pattern = r"^[a-zA-Z]{2}(?:[a-zA-Z]+)?\d-"
        df["Usage_type"] = df["Usage_type"].str.replace(region_pattern, "", regex=True)
