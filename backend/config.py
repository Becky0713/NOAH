from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application configuration loaded from environment variables and .env file."""

    # General
    app_name: str = "NYC Housing Hub"
    environment: str = "development"
    data_provider: str = "socrata"  # example | socrata | census

    # CORS
    cors_allow_origins: str = "*"  # comma-separated list in production

    # HTTP client
    http_timeout_seconds: int = 20

    # Example API keys (replace with real keys once provided)
    example_api_key: Optional[str] = None

    # Socrata (NYC Open Data)
    socrata_base_url: str = "https://data.cityofnewyork.us"
    socrata_app_token: Optional[str] = None
    # Dataset and field mappings for normalization
    socrata_dataset_id: Optional[str] = None
    socrata_field_id: str = "id"
    socrata_field_address: str = "address"
    socrata_field_latitude: str = "latitude"
    socrata_field_longitude: str = "longitude"
    socrata_field_bedrooms: str = "bedrooms"
    socrata_field_bathrooms: str = "bathrooms"
    socrata_field_rent: str = "rent"
    socrata_field_region: str = "region"  # 用于按区域筛选（如 borough/neighborhood）

    # Census (placeholder)
    census_api_key: Optional[str] = None

    # Database
    db_path: str = "./data/nyc_housing.db"
    db_table_affordable_housing: str = "affordable_housing_buildings"

    # Default dataset for Socrata
    socrata_dataset_id: Optional[str] = "hg8x-zxpr"

    class Config:
        env_file = "../.env"
        env_file_encoding = "utf-8"


settings = Settings()



