"""Application configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the SSE service."""

    app_name: str = "pySSE"
    log_level: str = "INFO"
    event_interval_seconds: float = Field(default=2.0, gt=0)
    heartbeat_seconds: float = Field(default=15.0, gt=0)
    queue_size: int = Field(default=100, gt=0)
    event_type: str = "tick"
    replay_buffer_size: int = Field(default=200, gt=0)
    cors_allow_origins: list[str] = Field(default_factory=lambda: ["*"])

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="PYSSE_",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def parse_origins(cls, value: list[str] | str) -> list[str]:
        """Allow JSON arrays or comma-separated CORS origins in env vars."""

        if isinstance(value, str):
            value = value.strip()
            if value.startswith("["):
                return json.loads(value)
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value


def load_dotenv_file(dotenv_path: str = ".env") -> bool:
    """Load key/value pairs from a dotenv file into process environment.

    Returns:
        True when a dotenv file was found and parsed, otherwise False.
    """

    env_file = Path(dotenv_path)
    if not env_file.exists():
        return False

    return load_dotenv(dotenv_path=env_file, override=False)


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance after loading `.env` variables."""

    load_dotenv_file()
    return Settings()
