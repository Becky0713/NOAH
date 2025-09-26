from __future__ import annotations

from typing import Any, Dict, List

from .base import BaseHousingClient


class CensusHousingClient(BaseHousingClient):
    """US Census data client (placeholder).

    Description:
    - Needs implementation based on specific datasets (e.g., ACS 5-year) and geographic levels (tract, block group, county, etc.).
    - Currently only maintains interface structure for future integration.
    """

    async def fetch_region_summary(self, region_id: str) -> Dict[str, Any]:
        # TODO: Map region_id to FIPS/GeoID and query statistics
        return {
            "region": {"id": region_id, "name": region_id.replace("_", " ").title()},
            "listing_count": 0,
            "median_rent": None,
            "average_rent": None,
            "vacancy_rate": None,
        }

    async def fetch_listings(self, region_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        # Census is not listing-level data, return empty list as placeholder
        return []



