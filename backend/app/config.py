"""Configuration settings for the Data QA Agent backend."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""
    
    # Application
    app_name: str = "Data QA Agent Backend"
    debug: bool = False
    
    # Google Cloud
    google_cloud_project: str = "default-project"
    vertex_ai_location: str = "us-central1"
    vertex_ai_model: str = "gemini-1.5-flash-001"
    
    # CORS
    cors_origins: list[str] = [
        "http://localhost:3000",
        "https://data-qa-agent-*.run.app"
    ]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
