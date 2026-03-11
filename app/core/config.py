import os
from dotenv import load_dotenv
load_dotenv()
class Settings:
    api_port: int = int(os.getenv("API_PORT", "8080"))
    database_url: str = os.getenv("DATABASE_URL")
    export_storage_path: str = os.getenv("EXPORT_STORAGE_PATH", "/app/exports")
settings = Settings()
