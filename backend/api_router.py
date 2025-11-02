from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status

from .clients.example_client import ExampleHousingClient
from .clients.socrata_client import SocrataHousingClient
from .models import ApiError, FieldMetadata, Region, SummaryResponse

# Import database client only if available
try:
    from .clients.database_client import DatabaseHousingClient
    DATABASE_AVAILABLE = True
except ImportError:
    DatabaseHousingClient = None
    DATABASE_AVAILABLE = False


router = APIRouter()


async def get_client():
    # Switch based on provider config
    from .main import http_client
    from .config import settings

    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client not initialized")

    provider = settings.data_provider.lower()
    if provider == "database":
        if not DATABASE_AVAILABLE:
            raise HTTPException(
                status_code=500, 
                detail="Database provider requested but database dependencies not available. Please use 'socrata' or 'example' provider."
            )
        return DatabaseHousingClient()
    elif provider == "socrata":
        return SocrataHousingClient(http_client)
    # elif provider == "census":
    #     return CensusHousingClient(http_client)
    else:
        return ExampleHousingClient(http_client)


@router.get("/health", tags=["system"])
async def health() -> dict:
    return {"status": "ok"}


@router.get("/v1/regions", response_model=List[Region], tags=["metadata"])
async def list_regions() -> List[Region]:
    # Placeholder list; replace with NYC boroughs + neighborhoods from metadata API later
    return [
        Region(id="manhattan", name="Manhattan"),
        Region(id="brooklyn", name="Brooklyn"),
        Region(id="queens", name="Queens"),
        Region(id="bronx", name="Bronx"),
        Region(id="staten_island", name="Staten Island"),
    ]


@router.get(
    "/v1/housing/summary",
    response_model=SummaryResponse,
    responses={404: {"model": ApiError}},
    tags=["housing"],
)
async def get_housing_summary(
    region_id: str,
    limit: int = 25,
    client: ExampleHousingClient = Depends(get_client),
) -> SummaryResponse:
    summary_dict = await client.fetch_region_summary(region_id)
    listings_dict = await client.fetch_listings(region_id, limit=limit)
    return SummaryResponse(region_summary=summary_dict, listings_sample=listings_dict)


# New endpoints
@router.get("/metadata/fields", response_model=List[FieldMetadata], tags=["metadata"])
async def list_fields(client=Depends(get_client)) -> List[FieldMetadata]:
    # Only Socrata client implements metadata; others can raise 400
    if not hasattr(client, "fetch_metadata_fields"):
        raise HTTPException(status_code=400, detail="Metadata not supported for current provider")
    fields = await client.fetch_metadata_fields()
    return [FieldMetadata(**f) for f in fields]


@router.get("/v1/records", tags=["housing"])
async def list_records(
    fields: str = Query(
        default="house_number,street_name,latitude,longitude,borough,total_units,all_counted_units,project_start_date,project_completion_date,studio_units,project_name,postcode",
        description="Comma-separated field names that will be merged with core fields",
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    borough: str = Query(default="", description="Filter by borough (empty string means no filter)"),
    min_units: int = Query(default=0, ge=0, description="Minimum unit count"),
    max_units: int = Query(default=0, ge=0, description="Maximum unit count (0 means no limit)"),
    start_date_from: str = Query(default="", description="Project start date from (YYYY-MM-DD)"),
    start_date_to: str = Query(default="", description="Project start date to (YYYY-MM-DD)"),
    client=Depends(get_client),
):
    # Build select list: core subset + any additional
    requested = [f.strip() for f in fields.split(",") if f.strip()]
    # Deduplicate while preserving order
    seen = set()
    selected: List[str] = []
    for name in requested:
        if name not in seen:
            selected.append(name)
            seen.add(name)

    if not hasattr(client, "fetch_records"):
        raise HTTPException(status_code=400, detail="Records not supported for current provider")
    data = await client.fetch_records(
        selected, 
        limit=limit, 
        offset=offset, 
        borough=borough,
        min_units=min_units,
        max_units=max_units,
        start_date_from=start_date_from,
        start_date_to=start_date_to
    )
    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail="Upstream provider error or rate limit; ensure SOCRATA_APP_TOKEN is set.")

    # Expose core subset under normalized keys for the frontend
    result: List[dict] = []
    for row in data:
        if not isinstance(row, dict):
            # skip invalid rows defensively
            continue
        address = None
        if "house_number" in row or "street_name" in row:
            hn = str(row.get("house_number", "")).strip()
            sn = str(row.get("street_name", "")).strip()
            address = (hn + " " + sn).strip() if hn or sn else None
        result.append(
            {
                "project_id": row.get("project_id"),
                "address": address,
                "latitude": _safe_float(row.get("latitude")),
                "longitude": _safe_float(row.get("longitude")),
                "region": row.get("borough"),
                "borough": row.get("borough"),
                "total_units": _safe_int(row.get("total_units")),
                "affordable_units": _safe_int(row.get("all_counted_units")),
                "project_start_date": row.get("project_start_date"),
                "project_completion_date": row.get("project_completion_date"),
                "studio_units": _safe_int(row.get("studio_units")),
                "project_name": row.get("project_name"),
                "postcode": row.get("postcode"),
                "_raw": row,
            }
        )
    return result


def _safe_float(v):
    try:
        return float(v) if v is not None and v != "" else None
    except Exception:  # noqa: BLE001
        return None


def _safe_int(v):
    try:
        return int(float(v)) if v is not None and v != "" else None
    except Exception:  # noqa: BLE001
        return None


@router.get("/database/stats", tags=["database"])
async def get_database_stats(client=Depends(get_client)):
    """Get database statistics"""
    if not DATABASE_AVAILABLE:
        raise HTTPException(status_code=400, detail="Database dependencies not available")
    
    if not hasattr(client, "get_database_stats"):
        raise HTTPException(status_code=400, detail="Database stats not supported for current provider")
    
    try:
        stats = await client.get_database_stats()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/rent-burden", tags=["rent-burden"])
def get_rent_burden_data():
    """Get rent burden data for choropleth visualization"""
    try:
        import psycopg2
        import pandas as pd
        from ..config import settings
        
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password
        )
        
        query = """
        SELECT 
            geo_id,
            tract_name,
            rent_burden_rate,
            severe_burden_rate
        FROM rent_burden
        WHERE rent_burden_rate IS NOT NULL;
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching rent burden data: {str(e)}")



