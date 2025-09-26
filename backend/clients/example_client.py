from __future__ import annotations

from typing import Any, Dict, List

from .base import BaseHousingClient


class ExampleHousingClient(BaseHousingClient):
    """Placeholder client that simulates a provider until real APIs are wired."""

    async def fetch_region_summary(self, region_id: str) -> Dict[str, Any]:
        # Mocked data; replace with real API aggregation when available
        return {
            "region": {"id": region_id, "name": region_id.replace("_", " ").title()},
            "listing_count": 1234,
            "median_rent": 3200.0,
            "average_rent": 3350.5,
            "vacancy_rate": 0.045,
        }

    async def fetch_listings(self, region_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        # Return a small synthetic sample
        result: List[Dict[str, Any]] = []
        for index in range(min(limit, 25)):
            result.append(
                {
                    "id": f"{region_id}-L{index}",
                    "address": f"{100 + index} Example St",
                    "latitude": 40.7 + index * 0.001,
                    "longitude": -73.95 - index * 0.001,
                    "bedrooms": 1 + (index % 3),
                    "bathrooms": 1 + (index % 2) * 0.5,
                    "rent": 2500 + (index * 50),
                    "source": "example",
                }
            )
        return result



