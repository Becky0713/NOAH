from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential


class BaseHousingClient(ABC):
    """Abstract client defining the interface for housing data providers."""

    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self.http = http_client

    @abstractmethod
    async def fetch_region_summary(self, region_id: str) -> Dict[str, Any]:
        """Return normalized summary dict for the given region_id."""
        raise NotImplementedError

    @abstractmethod
    async def fetch_listings(self, region_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a list of normalized listings dictionaries for the region."""
        raise NotImplementedError

    async def _get_with_retries(self, url: str, **kwargs: Any) -> httpx.Response:
        async for attempt in AsyncRetrying(
            reraise=True,
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
            retry=retry_if_exception_type((httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError)),
        ):
            with attempt:
                return await self.http.get(url, **kwargs)
        raise RuntimeError("Unreachable")



