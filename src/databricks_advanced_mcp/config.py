"""Configuration management using Pydantic settings.

Reads Databricks connection parameters from environment variables or .env file.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Databricks connection
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_warehouse_id: str = ""

    # Default catalog/schema for unqualified table names
    databricks_catalog: str = "main"
    databricks_schema: str = "default"

    # Graph cache staleness threshold in seconds (default: 1 hour)
    graph_cache_ttl: int = 3600

    # Automatic background graph refresh interval in seconds.
    # Set to 0 (default) to disable auto-refresh.
    # When > 0, the server will rebuild the dependency graph in the background
    # on this interval (e.g. set to 3600 for hourly refresh).
    graph_refresh_interval: int = 0

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


# Module-level singleton
_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached settings singleton."""
    global _settings  # noqa: PLW0603
    if _settings is None:
        _settings = Settings()
    return _settings
