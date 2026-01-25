import logging
from typing import Any

import pandas as pd
from cachetools import TTLCache, cached
from cachetools.keys import hashkey

import aws_cost_tool.cost_explorer as ce
from aws_cost_tool.client import create_ce_client

logger = logging.getLogger(__name__)
#  Module-level cache
cost_cache: TTLCache[Any, Any] = TTLCache(maxsize=128, ttl=14400)


def clear_cost_cache():
    logger.info("Clearing cost cache")
    cost_cache.clear()


def cache_key(self, **kwargs):
    # Convert only the unhashable types (like lists) to tuples
    hashable_params = {k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}

    # Generates the final stable cache key based on argument values, excluding
    # the client object.
    return hashkey(frozenset(hashable_params.items()))


class AWSCostSource:
    def __init__(self, profile: str | None = None):
        self.client = create_ce_client(profile_name=profile)

    @cached(cache=cost_cache, key=cache_key)
    def get_tags_for_key(self, **kwargs) -> list[str]:
        return ce.get_tags_for_key(self.client, **kwargs)

    @cached(cache=cost_cache, key=cache_key)
    def fetch_service_costs(self, **kwargs) -> pd.DataFrame:
        logger.info(f"fetching service costs: {kwargs}")
        return ce.fetch_service_costs(self.client, **kwargs)

    @cached(cache=cost_cache, key=cache_key)
    def fetch_service_costs_by_usage(self, **kwargs) -> pd.DataFrame:
        logger.info(f"fetching service costs by usage: {kwargs}")
        return ce.fetch_service_costs_by_usage(self.client, **kwargs)
