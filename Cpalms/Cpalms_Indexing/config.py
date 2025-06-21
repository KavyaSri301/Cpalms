import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    AZURE_SEARCH_ENDPOINT: str = os.getenv("AZURE_SEARCH_ENDPOINT", "")
    AZURE_SEARCH_API_KEY: str = os.getenv("AZURE_SEARCH_API_KEY", "")
    AZURE_SEARCH_INDEX_NAME: str = os.getenv("AZURE_SEARCH_INDEX_NAME", "resource-index")
                                                                                                               
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    AZURE_STORAGE_CONTAINERS: str = os.getenv("AZURE_STORAGE_CONTAINERS", "cplamsfiles,cplamsnewdata")
    AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    AZURE_OPENAI_API_KEY: str = os.getenv("AZURE_OPENAI_API_KEY", "")
    AZURE_OPENAI_API_VERSION: str = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT: str = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-large")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_EMBEDDING_MODEL: str = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large")
    
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY: int = int(os.getenv("RETRY_DELAY", "5"))
    MAX_FILES_PER_BENCHMARK: int = int(os.getenv("MAX_FILES_PER_BENCHMARK", "20"))
    MAX_TOTAL_FILES: int = int(os.getenv("MAX_TOTAL_FILES", "500"))
    PROCESSING_TIMEOUT_MINUTES: int = int(os.getenv("PROCESSING_TIMEOUT_MINUTES", "30"))
    
    def __post_init__(self):
        """Validate required settings after initialization"""
        required_fields = [
            "AZURE_SEARCH_ENDPOINT",
            "AZURE_SEARCH_API_KEY", 
            "AZURE_STORAGE_CONNECTION_STRING"
        ]
        
        if self.AZURE_OPENAI_ENDPOINT and self.AZURE_OPENAI_API_KEY:
            required_fields.extend([
                "AZURE_OPENAI_ENDPOINT",
                "AZURE_OPENAI_API_KEY",
                "AZURE_OPENAI_EMBEDDING_DEPLOYMENT"
            ])
        elif self.OPENAI_API_KEY:
            required_fields.append("OPENAI_API_KEY")
        else:
            required_fields.append("AZURE_OPENAI_API_KEY or OPENAI_API_KEY")
        
        missing_fields = []
        for field in required_fields:
            if "or" in field:
                continue
            if not getattr(self, field):
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_fields)}")
    
    def get_storage_containers(self) -> List[str]:
        """Parse comma-separated container names into a list"""
        return [container.strip() for container in self.AZURE_STORAGE_CONTAINERS.split(",")]
    
    def is_azure_openai_configured(self) -> bool:
        """Check if Azure OpenAI is properly configured"""
        return bool(self.AZURE_OPENAI_ENDPOINT and self.AZURE_OPENAI_API_KEY and self.AZURE_OPENAI_EMBEDDING_DEPLOYMENT)

settings = Settings()