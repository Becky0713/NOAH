"""
Environment-aware settings configuration
Supports multiple deployment environments with easy migration
"""
import os
from typing import List, Optional
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Data Provider Configuration
    data_provider: str = "socrata"
    socrata_app_token: Optional[str] = None
    socrata_base_url: str = "https://data.cityofnewyork.us/resource"
    socrata_dataset_id: str = "hg8x-zxpr"
    
    # Server Configuration
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    
    # CORS Configuration
    allowed_origins: List[str] = [
        "http://localhost:8501",
        "http://127.0.0.1:8501",
        "https://share.streamlit.io"
    ]
    
    # Database Configuration
    database_url: str = "sqlite:///./data/housing.db"
    
    # Frontend Configuration
    frontend_url: str = "http://localhost:8501"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    def get_cors_origins(self) -> List[str]:
        """Get CORS origins from environment or default"""
        if "ALLOWED_ORIGINS" in os.environ:
            return os.environ["ALLOWED_ORIGINS"].split(",")
        return self.allowed_origins


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get settings instance (for dependency injection)"""
    return settings
