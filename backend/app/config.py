from pydantic_settings import BaseSettings
from typing import Optional, List

class Settings(BaseSettings):
    # Google Cloud Settings
    google_cloud_project: str = "your-project-id"
    google_cloud_region: str = "us-central1"
    
    # Vertex AI Settings
    vertex_ai_model: str = "gemini-1.5-pro"
    
    # BigQuery Settings
    bq_dataset: str = "qa_agent"
    bq_config_table: str = "validation_config"
    bq_history_table: str = "execution_history"
    bq_custom_tests_table: str = "custom_tests"
    bq_settings_table: str = "project_settings"
    
    # Application Settings
    app_name: str = "Data QA Agent API"
    debug: bool = False
    
    # Cloud Run / Deployment
    cloud_run_url: Optional[str] = None
    cors_origins: List[str] = ["*"]
    
    class Config:
        env_file = ".env"

settings = Settings()
