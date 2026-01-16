"""Configuration settings for the Data QA Agent backend."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""
    
    # Application
    app_name: str = "Data QA Agent Backend"
    debug: bool = False
    
    # Google Cloud
    google_cloud_project: str = "leyin-sandpit"
    google_cloud_region: str = "us-central1"
    cloud_run_url: Optional[str] = None
    vertex_ai_location: str = "us-central1"
    vertex_ai_model: str = "gemini-1.5-flash"
    scheduler_location: str = "us-central1"
    scheduler_timezone: str = "Australia/Melbourne"
    
    # CORS
    cors_origins: list[str] = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
