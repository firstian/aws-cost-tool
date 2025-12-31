import re
import unicodedata
from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import reduce

import pandas as pd

type Extractor = Callable[[pd.DataFrame], pd.DataFrame]


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
    def slugify_name(self) -> str:
        """
        Normalizes a string to be filesystem-friendly:
        lowercase, no spaces, no special characters.
        """
        # Convert to lowercase and normalize unicode (e.g., convert 'é' to 'e')
        text = self.shortname.lower()
        text = (
            unicodedata.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode("ascii")
        )

        # Replace any non-alphanumeric characters with a hyphen
        # [^a-z0-9]+ matches any sequence of characters that AREN'T a-z or 0-9
        text = re.sub(r"[^a-z0-9]+", "-", text)

        # Remove leading/trailing hyphens
        return text.strip("-")

    @abstractmethod
    def categorize_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Logic to create a multi-index the dataframe with categorized usage"""
        ...

    def categorize_usage_costs(
        self, df: pd.DataFrame, *, extractors: dict[str, Extractor]
    ) -> pd.DataFrame:
        """
        A utility that creates a categorized version of usage cost DataFrame,
        given a table of extractors. The key of the extractors are used in the
        level 0 index of the returned DataFrame.
        """
        if df.empty:
            return pd.DataFrame()
        groups = {key: func(df) for key, func in extractors.items()}
        indices = [df.index for df in groups.values()]
        union_index = reduce(lambda x, y: x.union(y), indices)
        other_index = df.index.difference(union_index)
        if not other_index.empty:
            other_df = df.loc[other_index]
            other_df["Subtype"] = "Other"
            groups["Other"] = other_df
        final_df = pd.concat(groups)
        final_df.index.names = ["Category", "OriginalIndex"]
        return final_df
