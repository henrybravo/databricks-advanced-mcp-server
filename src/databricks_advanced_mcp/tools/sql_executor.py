"""SQL execution tools for the Databricks MCP server.

Provides the execute_query tool that runs SQL against a Databricks SQL warehouse.
"""

from __future__ import annotations

import json
from typing import Any

from databricks.sdk.service.sql import StatementState

from databricks_advanced_mcp.client import get_workspace_client
from databricks_advanced_mcp.config import get_settings

try:
    from fastmcp import FastMCP
except ImportError:  # pragma: no cover
    pass


def register(mcp: FastMCP) -> None:
    """Register SQL executor tools on the MCP server."""

    @mcp.tool()
    def execute_query(query: str, limit: int = 1000) -> str:
        """Execute a SQL query against a Databricks SQL warehouse.

        Runs any SQL statement (SELECT, DDL, DML) and returns structured results.
        Results are limited to the specified maximum row count.

        Args:
            query: SQL query to execute.
            limit: Maximum number of rows to return (default: 1000).

        Returns:
            JSON string with query results, columns, and metadata.
        """
        client = get_workspace_client()
        settings = get_settings()

        try:
            response = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=settings.databricks_warehouse_id,
                catalog=settings.databricks_catalog,
                schema=settings.databricks_schema,
                row_limit=limit,
                wait_timeout="30s",
            )
        except Exception as e:
            return json.dumps({"error": str(e), "query": query}, indent=2)

        if response.status and response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            return json.dumps(
                {
                    "error": error_msg,
                    "query": query,
                    "state": "FAILED",
                },
                indent=2,
            )

        # Extract column names
        columns: list[str] = []
        if response.manifest and response.manifest.schema and response.manifest.schema.columns:
            columns = [col.name for col in response.manifest.schema.columns if col.name]

        # Extract rows
        rows: list[dict[str, Any]] = []
        if response.result and response.result.data_array:
            for row_data in response.result.data_array:
                row = {}
                for i, value in enumerate(row_data):
                    col_name = columns[i] if i < len(columns) else f"col_{i}"
                    row[col_name] = value
                rows.append(row)

        # Check truncation
        truncated = False
        total_row_count = len(rows)
        if response.manifest and response.manifest.truncated:
            truncated = True
            if response.manifest.total_row_count:
                total_row_count = response.manifest.total_row_count

        result: dict[str, Any] = {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }
        if truncated:
            result["total_row_count"] = total_row_count

        return json.dumps(result, indent=2, default=str)
