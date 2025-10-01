from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base import BaseHousingClient
from ..config import settings


class SocrataHousingClient(BaseHousingClient):
    """Socrata (NYC Open Data) client.

    Description:
    - Uses SoQL query parameters: $select, $where, $limit, $order, etc.
    - Depends on dataset and field mapping in configuration for normalization
    """

    def _dataset_url(self) -> str:
        if not settings.socrata_dataset_id:
            raise ValueError("Missing socrata_dataset_id configuration")
        return f"{settings.socrata_base_url}/resource/{settings.socrata_dataset_id}.json"

    def _metadata_url(self) -> str:
        if not settings.socrata_dataset_id:
            raise ValueError("Missing socrata_dataset_id configuration")
        # Socrata metadata endpoint
        return f"{settings.socrata_base_url}/api/views/{settings.socrata_dataset_id}"

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if settings.socrata_app_token:
            headers["X-App-Token"] = settings.socrata_app_token
        return headers

    def _normalize_listing(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item.get(settings.socrata_field_id),
            "address": item.get(settings.socrata_field_address),
            "latitude": None,  # This dataset doesn't have coordinates
            "longitude": None,  # This dataset doesn't have coordinates
            "bedrooms": None,  # This dataset doesn't have bedroom info
            "bathrooms": None,  # This dataset doesn't have bathroom info
            "rent": self._to_float(item.get(settings.socrata_field_rent)),
            "source": "socrata",
        }

    @staticmethod
    def _to_float(value: Optional[str]) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _to_int(value: Optional[str]) -> Optional[int]:
        try:
            return int(float(value)) if value is not None else None
        except Exception:  # noqa: BLE001
            return None

    async def fetch_region_summary(self, region_id: str) -> Dict[str, Any]:
        # This dataset doesn't have borough/region filtering, so return total counts
        url = self._dataset_url()
        
        # Count total projects
        params_count = {"$select": "count(1) as listing_count"}
        resp_count = await self._get_with_retries(url, headers=self._headers(), params=params_count)
        data_count = resp_count.json()
        listing_count = int(data_count[0].get("listing_count", 0)) if data_count and len(data_count) > 0 else 0

        region = {"id": region_id, "name": region_id.replace("_", " ").title()}
        return {
            "region": region,
            "listing_count": listing_count,
            "median_rent": None,  # Not applicable for housing units
            "average_rent": None,  # Not applicable for housing units
            "vacancy_rate": None,
        }

    async def fetch_listings(self, region_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        url = self._dataset_url()
        # This dataset doesn't have region filtering, so get all projects
        params = {"$limit": limit, "$order": f"{settings.socrata_field_rent} DESC"}
        resp = await self._get_with_retries(url, headers=self._headers(), params=params)
        data = resp.json()
        return [self._normalize_listing(item) for item in data]

    async def fetch_metadata_fields(self) -> List[Dict[str, Any]]:
        url = self._metadata_url()
        resp = await self._get_with_retries(url, headers=self._headers())
        meta = resp.json()
        columns = meta.get("columns", [])
        results: List[Dict[str, Any]] = []
        for col in columns:
            field_name = col.get("fieldName")
            description = col.get("description") or col.get("name")
            data_type = col.get("dataTypeName")
            if field_name:
                results.append({
                    "field_name": field_name,
                    "data_type": data_type,
                    "description": description,
                })
        return results

    async def fetch_records(
        self, 
        fields: List[str], 
        limit: int = 100, 
        offset: int = 0, 
        borough: str = "",
        min_units: int = 0,
        max_units: int = 0,
        start_date_from: str = "",
        start_date_to: str = ""
    ) -> List[Dict[str, Any]]:
        url = self._dataset_url()
        select_clause = ", ".join(fields)
        params = {
            "$select": select_clause,
            "$limit": limit,
            "$offset": offset,
        }
        
        # Build WHERE clause with multiple conditions
        where_conditions = []
        if borough:
            where_conditions.append(f"borough = '{borough}'")
        if min_units > 0:
            where_conditions.append(f"total_units >= {min_units}")
        if max_units > 0:
            where_conditions.append(f"total_units <= {max_units}")
        if start_date_from:
            where_conditions.append(f"project_start_date >= '{start_date_from}'")
        if start_date_to:
            where_conditions.append(f"project_start_date <= '{start_date_to}'")
        
        if where_conditions:
            params["$where"] = " AND ".join(where_conditions)
        resp = await self._get_with_retries(url, headers=self._headers(), params=params)
        try:
            data = resp.json()
        except Exception:  # noqa: BLE001
            return []
        if isinstance(data, list):
            return data
        # If dict (error structure) or other type, return empty list to avoid 500
        return []



