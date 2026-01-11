import logging
from pathlib import Path

import pandas as pd

from aws_cost_tool.ce_types import CostMetric, DateRange, Granularity
from aws_cost_tool.service_loader import get_service

logger = logging.getLogger(__name__)


class FileDataSource:
    """
    This DataSource is a simple shim that returns DataFrame sourced from some
    CSV data. It ignores all the dates and granularity parameters. It is only
    useful for ad hoc manual testing, and thus not that robust.
    """

    def __init__(self, data_dir: Path):
        self.cost_data: dict[str, pd.DataFrame] = {}
        if not data_dir.is_dir():
            raise FileNotFoundError(f"The directory {data_dir} does not exist.")

        for file_path in data_dir.glob("*.csv"):
            try:
                df = pd.read_csv(file_path).fillna("")
                if not df.empty:
                    df["StartDate"] = pd.to_datetime(df["StartDate"]).dt.date
                    df["EndDate"] = pd.to_datetime(df["EndDate"]).dt.date
                    self.cost_data[file_path.stem.lower()] = df

            except pd.errors.EmptyDataError:
                logger.error(f"Error: {file_path.name} has no data to parse.")
            except pd.errors.ParserError:
                logger.error(f"Error: {file_path.name} is malformed.")
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while loading {file_path.name}: {e}"
                )
        if self.cost_data.get("cost_df") is None:
            raise ValueError("Main cost data file not found!")

        self.tags = sorted(self.cost_data["cost_df"]["Tag"].unique())

    def get_tags_for_key(self, *, tag_key: str, dates: DateRange) -> list[str]:
        return self.tags

    def fetch_service_costs(
        self,
        *,
        dates: DateRange,
        tag_key: str = "",
        cost_metric: CostMetric,
        granularity: Granularity,
    ) -> pd.DataFrame:
        return self.cost_data["cost_df"]

    def fetch_service_costs_by_usage(
        self,
        *,
        service: str,
        dates: DateRange,
        tag_key: str = "",
        cost_metric: CostMetric,
        granularity: Granularity,
    ) -> pd.DataFrame:
        s = get_service(service)
        if s is not None:
            df = self.cost_data.get(s.shortname.lower())
            if df is not None:
                return df
        # If we don't have any a service implementation, or we don't have the
        # data from file, then we return an empty DataFrame.
        return pd.DataFrame()
