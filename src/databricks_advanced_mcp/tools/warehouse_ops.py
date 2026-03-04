"""SQL Warehouse operations MCP tools.

Provides tools for listing SQL warehouses and managing their lifecycle
(start, stop).
"""

from __future__ import annotations

import json
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client


def register(mcp: FastMCP) -> None:
    """Register warehouse operations tools with the MCP server."""

    @mcp.tool()
    def list_warehouses(name_filter: str = "") -> str:
        """List all SQL warehouses with state, size, and type.

        Args:
            name_filter: Optional substring filter — only warehouses whose
                         name contains this string (case-insensitive) are returned.

        Returns:
            JSON with warehouse summaries.
        """
        client = get_workspace_client()

        try:
            warehouses = list(client.warehouses.list())
        except Exception as e:
            return json.dumps({"error": f"Failed to list warehouses: {e}"})

        results: list[dict[str, Any]] = []
        for w in warehouses:
            name = w.name or ""
            if name_filter and name_filter.lower() not in name.lower():
                continue
            results.append({
                "id": w.id,
                "name": name,
                "state": str(w.state) if w.state else "UNKNOWN",
                "cluster_size": w.cluster_size or "",
                "warehouse_type": str(w.warehouse_type) if w.warehouse_type else "",
                "creator_name": w.creator_name or "",
                "num_clusters": w.num_clusters or 1,
                "auto_stop_mins": w.auto_stop_mins or 0,
                "enable_serverless_compute": w.enable_serverless_compute or False,
            })

        return json.dumps({
            "warehouse_count": len(results),
            "warehouses": results,
        }, indent=2)

    @mcp.tool()
    def get_warehouse_status(warehouse_id: str) -> str:
        """Get detailed status and configuration for a SQL warehouse.

        Args:
            warehouse_id: The ID of the SQL warehouse to inspect.

        Returns:
            JSON with warehouse details including state, scaling config,
            and auto-stop settings.
        """
        client = get_workspace_client()

        try:
            w = client.warehouses.get(warehouse_id)
        except Exception as e:
            return json.dumps({"error": f"Failed to get warehouse '{warehouse_id}': {e}"})

        result: dict[str, Any] = {
            "id": w.id,
            "name": w.name or "",
            "state": str(w.state) if w.state else "UNKNOWN",
            "cluster_size": w.cluster_size or "",
            "warehouse_type": str(w.warehouse_type) if w.warehouse_type else "",
            "creator_name": w.creator_name or "",
            "num_clusters": w.num_clusters or 1,
            "min_num_clusters": w.min_num_clusters or 1,
            "max_num_clusters": w.max_num_clusters or 1,
            "auto_stop_mins": w.auto_stop_mins or 0,
            "enable_serverless_compute": w.enable_serverless_compute or False,
            "spot_instance_policy": str(w.spot_instance_policy) if w.spot_instance_policy else "",
            "num_active_sessions": w.num_active_sessions or 0,
        }

        return json.dumps(result, indent=2)

    @mcp.tool()
    def start_warehouse(warehouse_id: str, confirm: bool = False) -> str:
        """Start a stopped SQL warehouse.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to start.

        Args:
            warehouse_id: The ID of the SQL warehouse to start.
            confirm: Set to True to actually start the warehouse.

        Returns:
            JSON with start preview or result.
        """
        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would start SQL warehouse '{warehouse_id}'.",
                "warning": "Set confirm=True to actually start the warehouse.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.warehouses.start(warehouse_id)
            return json.dumps({
                "action": "started",
                "warehouse_id": warehouse_id,
                "status": "start command sent — warehouse is now starting",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to start warehouse '{warehouse_id}': {e}"})

    @mcp.tool()
    def stop_warehouse(warehouse_id: str, confirm: bool = False) -> str:
        """Stop a running SQL warehouse.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to stop.

        Args:
            warehouse_id: The ID of the SQL warehouse to stop.
            confirm: Set to True to actually stop the warehouse.

        Returns:
            JSON with stop preview or result.
        """
        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would stop SQL warehouse '{warehouse_id}'.",
                "warning": "Set confirm=True to actually stop the warehouse.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.warehouses.stop(warehouse_id)
            return json.dumps({
                "action": "stopped",
                "warehouse_id": warehouse_id,
                "status": "stop command sent — warehouse is now stopping",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to stop warehouse '{warehouse_id}': {e}"})
