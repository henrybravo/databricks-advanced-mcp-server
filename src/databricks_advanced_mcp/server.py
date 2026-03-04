"""FastMCP server setup and CLI entry-point.

Creates the MCP server instance and registers all tool modules.
"""

from __future__ import annotations

import logging
import threading
import time

from fastmcp import FastMCP

from databricks_advanced_mcp.config import get_settings
from databricks_advanced_mcp.tools import register_all_tools

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "Databricks Advanced MCP",
    instructions=(
        "An advanced MCP server for Databricks workspace intelligence. "
        "Provides tools for SQL execution, table metadata, dependency scanning, "
        "impact analysis, notebook review, and job/pipeline operations."
    ),
)

# Register all tool modules against the server instance
register_all_tools(mcp)


def _auto_refresh_worker(interval_seconds: int) -> None:
    """Background thread that periodically rebuilds the dependency graph cache."""
    from databricks_advanced_mcp.client import get_workspace_client
    from databricks_advanced_mcp.graph.builder import GraphBuilder
    from databricks_advanced_mcp.graph.cache import GraphCache

    logger.info("Auto-refresh worker started (interval=%ds).", interval_seconds)
    while True:
        time.sleep(interval_seconds)
        try:
            logger.info("Auto-refresh: rebuilding dependency graph...")
            client = get_workspace_client()
            graph = GraphBuilder(client).build()
            GraphCache.get_instance().set(graph)
            logger.info("Auto-refresh: graph rebuild complete.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Auto-refresh: graph rebuild failed: %s", exc)


def main() -> None:
    """CLI entry-point for the databricks-mcp command."""
    settings = get_settings()

    if settings.graph_refresh_interval > 0:
        t = threading.Thread(
            target=_auto_refresh_worker,
            args=(settings.graph_refresh_interval,),
            daemon=True,
            name="graph-auto-refresh",
        )
        t.start()
        logger.info(
            "Graph auto-refresh enabled every %d seconds.",
            settings.graph_refresh_interval,
        )

    mcp.run()


if __name__ == "__main__":
    main()
