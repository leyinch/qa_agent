"""Configuration settings for the Data QA Agent backend."""
from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""
    
    # Application
    app_name: str = "Data QA Agent Backend"
    debug: bool = False
    
    # Google Cloud
    google_cloud_project: str = Field("your-project-id", env="GOOGLE_CLOUD_PROJECT")
    google_cloud_region: str = Field("us-central1", env="GOOGLE_CLOUD_REGION")
    cloud_run_url: Optional[str] = None
    vertex_ai_location: str = Field("us-central1", env="GOOGLE_CLOUD_REGION")
    vertex_ai_model: str = Field("gemini-1.5-flash", env="VERTEX_AI_MODEL")
    scheduler_location: str = Field("us-central1", env="GOOGLE_CLOUD_REGION")
    scheduler_timezone: str = Field("Australia/Melbourne", env="SCHEDULER_TIMEZONE")
    
    # CORS
    cors_origins: list[str] = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
