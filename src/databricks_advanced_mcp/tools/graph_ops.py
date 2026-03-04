"""Graph operations MCP tools.

Provides tools for building, querying, and refreshing the workspace
dependency graph.
"""

from __future__ import annotations

import json
import time
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client
from databricks_advanced_mcp.config import get_settings
from databricks_advanced_mcp.graph.builder import GraphBuilder
from databricks_advanced_mcp.graph.cache import GraphCache
from databricks_advanced_mcp.graph.models import NodeType


def register(mcp: FastMCP) -> None:
    """Register graph operations tools with the MCP server."""

    @mcp.tool()
    def build_dependency_graph(
        scope: str = "workspace",
        path: str = "",
    ) -> str:
        """Build the workspace dependency graph.

        Orchestrates scanning of all notebooks, jobs, and DLT pipelines
        to build a unified directed acyclic graph. Results are cached.

        Args:
            scope: Scan scope — "workspace" (full scan, default) or "path" (scoped to a notebook path prefix).
            path: Notebook path prefix when scope="path" (e.g. "/Workspace/Repos/team/project").
                  Ignored when scope="workspace".

        Returns:
            JSON with graph summary (node/edge counts, roots, leaves).
        """
        if scope not in ("workspace", "path"):
            return json.dumps({"error": f"Invalid scope '{scope}'. Use 'workspace' or 'path'."})

        if scope == "path" and not path:
            return json.dumps({"error": "scope='path' requires a non-empty 'path' parameter."})

        client = get_workspace_client()
        settings = get_settings()
        cache = GraphCache.get_instance()
        cache.ttl = settings.graph_cache_ttl

        builder = GraphBuilder(client)
        path_prefix = path if scope == "path" else ""
        graph = builder.build(path_prefix=path_prefix)
        cache.set(graph)

        summary = cache.summary()
        summary["built_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        summary["scope"] = scope
        if scope == "path":
            summary["path"] = path

        return json.dumps(summary, indent=2)

    @mcp.tool()
    def get_table_dependencies(table_name: str) -> str:
        """Get upstream and downstream dependencies for a table.

        Queries the cached dependency graph to find all assets that
        produce (upstream) or consume (downstream) the given table.

        Args:
            table_name: Fully-qualified table name (catalog.schema.table).

        Returns:
            JSON with upstream producers, downstream consumers, and paths.
        """
        cache = GraphCache.get_instance()
        graph = cache.get_or_stale()

        if graph is None:
            return json.dumps({
                "error": "Dependency graph not built yet. Run build_dependency_graph first.",
                "cache_status": cache.summary(),
            })

        is_stale = cache.is_stale()

        table_id = f"{NodeType.TABLE.value}::{table_name}"
        node = graph.get_node(table_id)

        if node is None:
            return json.dumps({
                "error": f"Table '{table_name}' not found in the dependency graph.",
                "hint": "The table may not be referenced by any scanned asset.",
            })

        upstream = graph.get_upstream(table_id)
        downstream = graph.get_downstream(table_id)

        def _node_info(node_id: str) -> dict[str, Any]:
            data = graph.get_node(node_id) or {}
            return {
                "id": node_id,
                "type": data.get("node_type", "unknown"),
                "name": data.get("name", ""),
                "fqn": data.get("fqn", ""),
            }

        return json.dumps({
            "table": table_name,
            "upstream_count": len(upstream),
            "upstream": [_node_info(n) for n in upstream],
            "downstream_count": len(downstream),
            "downstream": [_node_info(n) for n in downstream],
            "stale_warning": is_stale,
            "graph_timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(cache.timestamp)
            ) if cache.timestamp else None,
        }, indent=2)

    @mcp.tool()
    def refresh_graph() -> str:
        """Invalidate and rebuild the dependency graph cache.

        Forces a full rebuild of the dependency graph regardless of
        cache TTL status.

        Returns:
            JSON with updated graph summary and timestamp.
        """
        cache = GraphCache.get_instance()
        cache.invalidate()

        # Rebuild
        client = get_workspace_client()
        settings = get_settings()
        cache.ttl = settings.graph_cache_ttl

        builder = GraphBuilder(client)
        graph = builder.build()
        cache.set(graph)

        summary = cache.summary()
        summary["refreshed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        return json.dumps(summary, indent=2)
