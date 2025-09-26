from typing import List, Optional

from pydantic import BaseModel, Field


class Region(BaseModel):
    id: str = Field(..., description="Unique identifier for a NYC region (e.g., borough or neighborhood)")
    name: str


class Listing(BaseModel):
    id: str
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    rent: Optional[float] = None
    source: Optional[str] = None


class RegionSummary(BaseModel):
    region: Region
    listing_count: int
    median_rent: Optional[float] = None
    average_rent: Optional[float] = None
    vacancy_rate: Optional[float] = None


class ApiError(BaseModel):
    error: str
    detail: Optional[str] = None


class SummaryResponse(BaseModel):
    region_summary: RegionSummary
    listings_sample: List[Listing] = []


# New models for metadata and records
class FieldMetadata(BaseModel):
    field_name: str
    data_type: Optional[str] = None
    description: Optional[str] = None


class RecordsResponse(BaseModel):
    # Default core fields as requested; additional fields may be present per request
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    region: Optional[str] = None
    total_units: Optional[int] = None
    affordable_units: Optional[int] = None
    project_start_date: Optional[str] = None
    project_completion_date: Optional[str] = None



