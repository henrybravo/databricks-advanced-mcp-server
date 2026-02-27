"""Workspace listing MCP tools.

Provides tools for enumerating notebooks and other objects in the
Databricks workspace via the Workspace API.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from databricks.sdk.service.workspace import ObjectType
from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client

logger = logging.getLogger(__name__)


def _list_notebooks_iterative(
    client: Any,
    path: str,
    max_depth: int,
) -> list[dict[str, str]]:
    """List notebooks using iterative DFS traversal.

    Args:
        client: Databricks WorkspaceClient.
        path: Root workspace path to list from.
        max_depth: Maximum directory depth to traverse.

    Returns:
        List of dicts with ``path``, ``name``, and ``language`` for each notebook.
    """
    notebooks: list[dict[str, str]] = []
    # Stack entries: (directory_path, current_depth)
    stack: list[tuple[str, int]] = [(path, 0)]

    while stack:
        current_path, depth = stack.pop()

        try:
            objects = list(client.workspace.list(current_path))
        except Exception as exc:
            # Permission or not-found errors on subdirectories are non-fatal
            logger.warning("Failed to list workspace path %s: %s", current_path, exc)
            continue

        for obj in objects:
            obj_path = obj.path or ""
            obj_type = obj.object_type

            if obj_type == ObjectType.NOTEBOOK:
                language = ""
                if obj.language:
                    language = str(obj.language.value) if hasattr(obj.language, "value") else str(obj.language)
                notebooks.append({
                    "path": obj_path,
                    "name": obj_path.rsplit("/", 1)[-1] if obj_path else "",
                    "language": language,
                })
            elif obj_type == ObjectType.DIRECTORY and depth < max_depth:
                stack.append((obj_path, depth + 1))

    return notebooks


def register(mcp: FastMCP) -> None:
    """Register workspace listing tools with the MCP server."""

    @mcp.tool()
    def list_workspace_notebooks(path: str = "/", max_depth: int = 10) -> str:
        """List all notebooks in a Databricks workspace path.

        Recursively traverses the workspace directory tree and returns
        all notebooks found, with their paths, names, and languages.

        Args:
            path: Workspace path to list from (default: "/").
            max_depth: Maximum directory depth to recurse (default: 10).

        Returns:
            JSON with ``path``, ``notebooks`` array, and ``count``.
        """
        client = get_workspace_client()

        try:
            # Verify the root path exists by attempting to list it
            objects = list(client.workspace.list(path))
        except Exception as exc:
            error_msg = str(exc).lower()
            if "not found" in error_msg or "does not exist" in error_msg or "404" in error_msg:
                return json.dumps({"error": f"Path not found: {path}"})
            return json.dumps({"error": f"Failed to list workspace path: {exc}"})

        # Process objects from the initial listing
        notebooks: list[dict[str, str]] = []
        stack: list[tuple[str, int]] = []

        for obj in objects:
            obj_path = obj.path or ""
            obj_type = obj.object_type

            if obj_type == ObjectType.NOTEBOOK:
                language = ""
                if obj.language:
                    language = str(obj.language.value) if hasattr(obj.language, "value") else str(obj.language)
                notebooks.append({
                    "path": obj_path,
                    "name": obj_path.rsplit("/", 1)[-1] if obj_path else "",
                    "language": language,
                })
            elif obj_type == ObjectType.DIRECTORY and max_depth > 0:
                stack.append((obj_path, 1))

        # Continue DFS for subdirectories
        while stack:
            current_path, depth = stack.pop()

            try:
                sub_objects = list(client.workspace.list(current_path))
            except Exception as exc:
                logger.warning(
                    "Skipping inaccessible directory %s: %s", current_path, exc
                )
                continue

            for obj in sub_objects:
                obj_path = obj.path or ""
                obj_type = obj.object_type

                if obj_type == ObjectType.NOTEBOOK:
                    language = ""
                    if obj.language:
                        language = str(obj.language.value) if hasattr(obj.language, "value") else str(obj.language)
                    notebooks.append({
                        "path": obj_path,
                        "name": obj_path.rsplit("/", 1)[-1] if obj_path else "",
                        "language": language,
                    })
                elif obj_type == ObjectType.DIRECTORY and depth < max_depth:
                    stack.append((obj_path, depth + 1))

        return json.dumps({
            "path": path,
            "notebooks": notebooks,
            "count": len(notebooks),
        }, indent=2)
