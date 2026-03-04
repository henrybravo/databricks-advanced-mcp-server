"""Table information tools for the Databricks MCP server.

Provides tools to retrieve table metadata, list tables, and get column details
via the Unity Catalog API.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import Any

from databricks.sdk.service.sql import StatementState

from databricks_advanced_mcp.client import get_workspace_client
from databricks_advanced_mcp.config import get_settings

try:
    from fastmcp import FastMCP
except ImportError:  # pragma: no cover
    pass

logger = logging.getLogger(__name__)


def _describe_detail(
    full_name: str,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> tuple[int | None, int | None]:
    """Execute ``DESCRIBE DETAIL <table>`` and return (row_count, size_bytes).

    Returns (None, None) silently on any failure (warehouse down, permissions,
    views, etc.).
    """
    try:
        client = get_workspace_client()
        response = client.statement_execution.execute_statement(
            statement=f"DESCRIBE DETAIL `{full_name}`",
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            wait_timeout="30s",
        )

        if (
            not response.status
            or response.status.state != StatementState.SUCCEEDED
        ):
            return None, None

        # Build column name → index mapping
        columns: list[str] = []
        if response.manifest and response.manifest.schema and response.manifest.schema.columns:
            columns = [col.name for col in response.manifest.schema.columns if col.name]

        if not columns or not response.result or not response.result.data_array:
            return None, None

        col_map = {name: idx for idx, name in enumerate(columns)}
        row = response.result.data_array[0]

        def _safe_int(col_name: str) -> int | None:
            idx = col_map.get(col_name)
            if idx is None or idx >= len(row) or row[idx] is None:
                return None
            try:
                return int(row[idx])
            except (ValueError, TypeError):
                return None

        num_records = _safe_int("numRecords")
        size_in_bytes = _safe_int("sizeInBytes")
        return num_records, size_in_bytes

    except Exception:
        logger.debug("DESCRIBE DETAIL failed for %s — returning nulls", full_name, exc_info=True)
        return None, None


def register(mcp: FastMCP) -> None:
    """Register table info tools on the MCP server."""

    @mcp.tool()
    def get_table_info(
        table_name: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        """Get detailed metadata for a Databricks table.

        Returns schema (columns with types/nullability), row count, table properties,
        storage location, and table type.

        Args:
            table_name: Name of the table.
            catalog: Catalog name (defaults to DATABRICKS_CATALOG env var).
            schema: Schema/database name (defaults to DATABRICKS_SCHEMA env var).

        Returns:
            JSON string with table metadata.
        """
        client = get_workspace_client()
        settings = get_settings()
        cat = catalog or settings.databricks_catalog
        sch = schema or settings.databricks_schema

        # If the table_name is already fully qualified (catalog.schema.table), use it as-is
        parts = table_name.split(".")
        if len(parts) == 3:
            full_name = table_name
            cat = parts[0]
        elif len(parts) == 2:
            full_name = f"{cat}.{table_name}"
        else:
            full_name = f"{cat}.{sch}.{table_name}"

        try:
            table = client.tables.get(full_name)
        except Exception as e:
            return json.dumps(
                {
                    "error": f"Table not found: {full_name}",
                    "detail": str(e),
                },
                indent=2,
            )

        # Build column details
        columns: list[dict[str, Any]] = []
        if table.columns:
            for col in table.columns:
                columns.append(
                    {
                        "name": col.name,
                        "type": col.type_text or str(col.type_name),
                        "nullable": col.nullable if col.nullable is not None else True,
                        "comment": col.comment,
                        "is_partition_column": col.partition_index is not None,
                    }
                )

        # Build properties dict
        properties: dict[str, str] = {}
        if table.properties:
            properties = dict(table.properties)

        # Retrieve physical statistics via DESCRIBE DETAIL for non-view tables
        row_count: int | None = None
        size_bytes: int | None = None
        table_type_str = str(table.table_type) if table.table_type else ""
        if "VIEW" not in table_type_str.upper():
            row_count, size_bytes = _describe_detail(
                full_name=table.full_name or full_name,
                warehouse_id=settings.databricks_warehouse_id,
                catalog=cat,
                schema=sch,
            )

        # Fallback: surface row_count from table statistics properties
        # when DESCRIBE DETAIL did not return a value.
        if row_count is None and properties:
            stats_rows = properties.get("spark.sql.statistics.numRows")
            if stats_rows is not None:
                with contextlib.suppress(ValueError, TypeError):
                    row_count = int(stats_rows)
        if size_bytes is None and properties:
            stats_size = properties.get("spark.sql.statistics.totalSize")
            if stats_size is not None:
                with contextlib.suppress(ValueError, TypeError):
                    size_bytes = int(stats_size)

        result: dict[str, Any] = {
            "full_name": table.full_name or full_name,
            "table_type": table_type_str or None,
            "columns": columns,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "properties": properties,
            "storage_location": table.storage_location,
            "created_at": str(table.created_at) if table.created_at else None,
            "updated_at": str(table.updated_at) if table.updated_at else None,
            "comment": table.comment,
        }

        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def list_tables(
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        """List all tables in a Databricks catalog and schema.

        Returns table names, types, and comments.

        Args:
            catalog: Catalog name (defaults to DATABRICKS_CATALOG env var).
            schema: Schema/database name (defaults to DATABRICKS_SCHEMA env var).

        Returns:
            JSON string with list of tables.
        """
        client = get_workspace_client()
        settings = get_settings()
        cat = catalog or settings.databricks_catalog
        sch = schema or settings.databricks_schema

        try:
            tables_iter = client.tables.list(catalog_name=cat, schema_name=sch)
            tables: list[dict[str, Any]] = []
            for t in tables_iter:
                tables.append(
                    {
                        "name": t.name,
                        "full_name": t.full_name,
                        "table_type": str(t.table_type) if t.table_type else None,
                        "comment": t.comment,
                        "created_at": str(t.created_at) if t.created_at else None,
                    }
                )
        except Exception as e:
            return json.dumps(
                {
                    "error": f"Failed to list tables in {cat}.{sch}",
                    "detail": str(e),
                },
                indent=2,
            )

        return json.dumps(
            {
                "catalog": cat,
                "schema": sch,
                "tables": tables,
                "count": len(tables),
            },
            indent=2,
            default=str,
        )
