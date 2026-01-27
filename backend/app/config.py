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
    google_cloud_region: str = "australia-southeast2"
    vertex_ai_location: str = "australia-southeast2"
    vertex_ai_model: str = "gemini-1.5-flash-002"
    
    # CORS
    cors_origins: list[str] = [
        "http://localhost:3000",
        "https://data-qa-agent-*.run.app"
    ]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
