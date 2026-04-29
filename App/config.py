from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).parent / ".env"
# override=True fuerza los valores del .env sobre las vars de entorno vacías del sistema
load_dotenv(str(_ENV_FILE), override=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(_ENV_FILE), env_file_encoding="utf-8", extra="ignore")

    docs_folder: str = "./docs"
    llm_provider: str = "claude"
    anthropic_api_key: str = ""
    claude_model: str = "claude-sonnet-4-6"
    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "mistral"
    embedding_model: str = "all-MiniLM-L6-v2"
    chroma_path: str = "./data/chroma"
    sqlite_path: str = "./data/metadata.db"
    host: str = "0.0.0.0"
    port: int = 8000

    @property
    def docs_path(self) -> Path:
        return Path(self.docs_folder)

    @property
    def chroma_dir(self) -> Path:
        p = Path(self.chroma_path)
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def sqlite_url(self) -> str:
        p = Path(self.sqlite_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{p}"


settings = Settings()
