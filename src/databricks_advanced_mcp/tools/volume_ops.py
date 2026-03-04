"""Unity Catalog Volume operations MCP tools.

Provides tools for listing volumes, inspecting volume metadata, listing
files inside volumes, and reading volume file contents.
"""

from __future__ import annotations

import json
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client


def register(mcp: FastMCP) -> None:
    """Register volume operations tools with the MCP server."""

    @mcp.tool()
    def list_volumes(catalog: str, schema: str) -> str:
        """List Unity Catalog volumes in a given catalog and schema.

        Args:
            catalog: Name of the catalog.
            schema: Name of the schema.

        Returns:
            JSON with volume names, types, and storage locations.
        """
        client = get_workspace_client()

        try:
            volumes = list(client.volumes.list(
                catalog_name=catalog,
                schema_name=schema,
            ))
        except Exception as e:
            return json.dumps({"error": f"Failed to list volumes in '{catalog}.{schema}': {e}"})

        results: list[dict[str, Any]] = []
        for v in volumes:
            results.append({
                "name": v.name,
                "full_name": v.full_name or f"{catalog}.{schema}.{v.name}",
                "volume_type": str(v.volume_type) if v.volume_type else "",
                "catalog_name": v.catalog_name or catalog,
                "schema_name": v.schema_name or schema,
                "storage_location": v.storage_location or "",
                "owner": v.owner or "",
                "comment": v.comment or "",
                "created_at": str(v.created_at) if v.created_at else None,
            })

        return json.dumps({
            "catalog": catalog,
            "schema": schema,
            "volume_count": len(results),
            "volumes": results,
        }, indent=2)

    @mcp.tool()
    def get_volume_info(volume_name: str, catalog: str, schema: str) -> str:
        """Get detailed metadata for a Unity Catalog volume.

        Args:
            volume_name: Name of the volume.
            catalog: Name of the parent catalog.
            schema: Name of the parent schema.

        Returns:
            JSON with volume metadata including type, storage location, and owner.
        """
        client = get_workspace_client()

        full_name = f"{catalog}.{schema}.{volume_name}"
        try:
            v = client.volumes.read(full_name)
        except Exception as e:
            return json.dumps({"error": f"Failed to get volume '{full_name}': {e}"})

        result: dict[str, Any] = {
            "name": v.name,
            "full_name": v.full_name or full_name,
            "volume_type": str(v.volume_type) if v.volume_type else "",
            "catalog_name": v.catalog_name or catalog,
            "schema_name": v.schema_name or schema,
            "storage_location": v.storage_location or "",
            "owner": v.owner or "",
            "comment": v.comment or "",
            "created_at": str(v.created_at) if v.created_at else None,
            "updated_at": str(v.updated_at) if v.updated_at else None,
        }

        return json.dumps(result, indent=2)

    @mcp.tool()
    def list_volume_files(volume_path: str) -> str:
        """List files and directories inside a Unity Catalog volume.

        Args:
            volume_path: Path inside the volume using the Volumes path format,
                         e.g. "/Volumes/catalog/schema/volume_name" or
                         "/Volumes/catalog/schema/volume_name/subdir".

        Returns:
            JSON with file/directory listing including names and sizes.
        """
        client = get_workspace_client()

        try:
            items = list(client.files.list_directory_contents(volume_path))
        except Exception as e:
            return json.dumps({"error": f"Failed to list volume path '{volume_path}': {e}"})

        results: list[dict[str, Any]] = []
        for item in items:
            results.append({
                "name": item.name or "",
                "path": item.path or "",
                "is_directory": item.is_directory or False,
                "file_size": item.file_size if item.file_size is not None else None,
                "last_modified": str(item.last_modified) if item.last_modified else None,
            })

        return json.dumps({
            "volume_path": volume_path,
            "item_count": len(results),
            "items": results,
        }, indent=2)

    @mcp.tool()
    def read_volume_file(volume_path: str, max_bytes: int = 1048576) -> str:
        """Read the contents of a file from a Unity Catalog volume.

        Reads up to max_bytes of the file. For binary files, returns
        base64-encoded content.

        Args:
            volume_path: Full path to the file, e.g.
                         "/Volumes/catalog/schema/volume/file.csv".
            max_bytes: Maximum number of bytes to read (default: 1 MB).

        Returns:
            JSON with file content (text or base64-encoded binary).
        """
        import base64

        client = get_workspace_client()

        try:
            response = client.files.download(volume_path)
            raw = response.contents.read(max_bytes)
        except Exception as e:
            return json.dumps({"error": f"Failed to read file '{volume_path}': {e}"})

        truncated = len(raw) >= max_bytes

        # Try decoding as UTF-8 text
        try:
            text = raw.decode("utf-8")
            return json.dumps({
                "volume_path": volume_path,
                "encoding": "utf-8",
                "content_length": len(raw),
                "truncated": truncated,
                "content": text,
            }, indent=2)
        except UnicodeDecodeError:
            # Fall back to base64 for binary files
            encoded = base64.b64encode(raw).decode("ascii")
            return json.dumps({
                "volume_path": volume_path,
                "encoding": "base64",
                "content_length": len(raw),
                "truncated": truncated,
                "content": encoded,
            }, indent=2)
