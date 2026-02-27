"""Databricks SDK client factory.

Provides a singleton WorkspaceClient configured via unified auth.
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

from databricks_advanced_mcp.config import get_settings

_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    """Return the cached WorkspaceClient singleton.

    Uses Databricks unified authentication — the SDK automatically picks up
    credentials from environment variables, .databrickscfg, Azure CLI, or
    managed identity.
    """
    global _client  # noqa: PLW0603
    if _client is None:
        settings = get_settings()
        kwargs: dict[str, str] = {}
        if settings.databricks_host:
            kwargs["host"] = settings.databricks_host
        if settings.databricks_token:
            kwargs["token"] = settings.databricks_token
        _client = WorkspaceClient(**kwargs)
    return _client
