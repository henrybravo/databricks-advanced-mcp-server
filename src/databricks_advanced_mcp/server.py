"""FastMCP server setup and CLI entry-point.

Creates the MCP server instance and registers all tool modules.
"""

from __future__ import annotations

from fastmcp import FastMCP

from databricks_advanced_mcp.tools import register_all_tools

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


def main() -> None:
    """CLI entry-point for the databricks-mcp command."""
    mcp.run()


if __name__ == "__main__":
    main()
