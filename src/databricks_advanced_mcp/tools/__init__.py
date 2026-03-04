"""Tool modules for the Databricks Advanced MCP server.

Each submodule defines tools via a `register(mcp)` function that attaches
@mcp.tool-decorated functions to the server instance.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastmcp import FastMCP


def register_all_tools(mcp: FastMCP) -> None:
    """Import and register all tool submodules."""
    from databricks_advanced_mcp.tools import (
        catalog_ops,
        compute_ops,
        dependency_scanner,
        graph_ops,
        impact_analysis,
        job_pipeline_ops,
        notebook_reviewer,
        sql_executor,
        table_info,
        volume_ops,
        warehouse_ops,
        workspace_listing,
        workspace_ops,
    )

    sql_executor.register(mcp)
    table_info.register(mcp)
    dependency_scanner.register(mcp)
    graph_ops.register(mcp)
    impact_analysis.register(mcp)
    notebook_reviewer.register(mcp)
    job_pipeline_ops.register(mcp)
    workspace_listing.register(mcp)
    workspace_ops.register(mcp)
    catalog_ops.register(mcp)
    compute_ops.register(mcp)
    warehouse_ops.register(mcp)
    volume_ops.register(mcp)
