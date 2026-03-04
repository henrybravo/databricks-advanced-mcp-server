"""Unity Catalog operations MCP tools.

Provides tools for listing catalogs, listing/describing/creating/dropping
schemas within Unity Catalog.
"""

from __future__ import annotations

import json
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client


def register(mcp: FastMCP) -> None:
    """Register catalog operations tools with the MCP server."""

    @mcp.tool()
    def list_catalogs() -> str:
        """List all Unity Catalog catalogs accessible to the current principal.

        Returns:
            JSON with catalog names, owners, types, and comments.
        """
        client = get_workspace_client()

        try:
            catalogs = list(client.catalogs.list())
        except Exception as e:
            return json.dumps({"error": f"Failed to list catalogs: {e}"})

        results: list[dict[str, Any]] = []
        for cat in catalogs:
            results.append({
                "name": cat.name,
                "owner": cat.owner or "",
                "comment": cat.comment or "",
                "catalog_type": str(cat.catalog_type) if cat.catalog_type else "MANAGED_CATALOG",
                "isolation_mode": str(cat.isolation_mode) if cat.isolation_mode else "",
                "created_at": str(cat.created_at) if cat.created_at else None,
            })

        return json.dumps({
            "catalog_count": len(results),
            "catalogs": results,
        }, indent=2)

    @mcp.tool()
    def list_schemas(catalog: str) -> str:
        """List all schemas in a Unity Catalog catalog.

        Args:
            catalog: Name of the catalog to list schemas from.

        Returns:
            JSON with schema names, owners, and comments.
        """
        client = get_workspace_client()

        try:
            schemas = list(client.schemas.list(catalog_name=catalog))
        except Exception as e:
            return json.dumps({"error": f"Failed to list schemas in '{catalog}': {e}"})

        results: list[dict[str, Any]] = []
        for schema in schemas:
            results.append({
                "name": schema.name,
                "full_name": schema.full_name or f"{catalog}.{schema.name}",
                "owner": schema.owner or "",
                "comment": schema.comment or "",
                "created_at": str(schema.created_at) if schema.created_at else None,
            })

        return json.dumps({
            "catalog": catalog,
            "schema_count": len(results),
            "schemas": results,
        }, indent=2)

    @mcp.tool()
    def describe_schema(catalog: str, schema: str) -> str:
        """Get detailed metadata for a Unity Catalog schema.

        Args:
            catalog: Name of the parent catalog.
            schema: Name of the schema to describe.

        Returns:
            JSON with schema metadata including owner, comment, and properties.
        """
        client = get_workspace_client()

        full_name = f"{catalog}.{schema}"
        try:
            info = client.schemas.get(full_name)
        except Exception as e:
            return json.dumps({"error": f"Failed to get schema '{full_name}': {e}"})

        result: dict[str, Any] = {
            "name": info.name,
            "full_name": info.full_name or full_name,
            "catalog_name": info.catalog_name or catalog,
            "owner": info.owner or "",
            "comment": info.comment or "",
            "properties": dict(info.properties) if info.properties else {},
            "created_at": str(info.created_at) if info.created_at else None,
            "updated_at": str(info.updated_at) if info.updated_at else None,
        }

        return json.dumps(result, indent=2)

    @mcp.tool()
    def create_schema(
        catalog: str,
        schema: str,
        comment: str = "",
        confirm: bool = False,
    ) -> str:
        """Create a new schema in a Unity Catalog catalog.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to create.

        Args:
            catalog: Name of the parent catalog.
            schema: Name of the schema to create.
            comment: Optional description for the schema.
            confirm: Set to True to actually create the schema.

        Returns:
            JSON with creation preview or result.
        """
        full_name = f"{catalog}.{schema}"

        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would create schema '{full_name}'.",
                "schema_config": {
                    "catalog": catalog,
                    "schema": schema,
                    "comment": comment,
                },
                "warning": "Set confirm=True to actually create the schema.",
            }, indent=2)

        client = get_workspace_client()

        try:
            info = client.schemas.create(
                name=schema,
                catalog_name=catalog,
                comment=comment or None,
            )
            return json.dumps({
                "action": "created",
                "full_name": info.full_name or full_name,
                "owner": info.owner or "",
                "comment": info.comment or "",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to create schema '{full_name}': {e}"})

    @mcp.tool()
    def drop_schema(
        catalog: str,
        schema: str,
        confirm: bool = False,
    ) -> str:
        """Drop a schema from a Unity Catalog catalog.

        The schema must be empty (no tables, views, or volumes).

        This is a DESTRUCTIVE operation. When confirm=False (default),
        returns a preview. Set confirm=True to drop.

        Args:
            catalog: Name of the parent catalog.
            schema: Name of the schema to drop.
            confirm: Set to True to actually drop the schema.

        Returns:
            JSON with drop preview or result.
        """
        full_name = f"{catalog}.{schema}"

        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would DROP schema '{full_name}'. Schema must be empty.",
                "schema_config": {
                    "catalog": catalog,
                    "schema": schema,
                },
                "warning": "⚠ DESTRUCTIVE: Set confirm=True to actually drop the schema.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.schemas.delete(full_name)
            return json.dumps({
                "action": "dropped",
                "full_name": full_name,
                "status": "schema deleted",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to drop schema '{full_name}': {e}"})
