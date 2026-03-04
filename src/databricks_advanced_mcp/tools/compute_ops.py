"""Compute (cluster) operations MCP tools.

Provides tools for listing clusters and managing their lifecycle
(start, stop, restart).
"""

from __future__ import annotations

import json
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client


def register(mcp: FastMCP) -> None:
    """Register compute operations tools with the MCP server."""

    @mcp.tool()
    def list_clusters(name_filter: str = "") -> str:
        """List all Databricks clusters with state, creator, and node type.

        Args:
            name_filter: Optional substring filter — only clusters whose name
                         contains this string (case-insensitive) are returned.

        Returns:
            JSON with cluster summaries.
        """
        client = get_workspace_client()

        try:
            clusters = list(client.clusters.list())
        except Exception as e:
            return json.dumps({"error": f"Failed to list clusters: {e}"})

        results: list[dict[str, Any]] = []
        for c in clusters:
            name = c.cluster_name or ""
            if name_filter and name_filter.lower() not in name.lower():
                continue
            results.append({
                "cluster_id": c.cluster_id,
                "cluster_name": name,
                "state": str(c.state) if c.state else "UNKNOWN",
                "creator_user_name": c.creator_user_name or "",
                "spark_version": c.spark_version or "",
                "node_type_id": c.node_type_id or "",
                "driver_node_type_id": c.driver_node_type_id or c.node_type_id or "",
                "autotermination_minutes": c.autotermination_minutes or 0,
                "num_workers": c.num_workers if c.num_workers is not None else None,
            })

        return json.dumps({
            "cluster_count": len(results),
            "clusters": results,
        }, indent=2)

    @mcp.tool()
    def get_cluster_status(cluster_id: str) -> str:
        """Get detailed status and configuration for a Databricks cluster.

        Args:
            cluster_id: The ID of the cluster to inspect.

        Returns:
            JSON with cluster details including state, spark version,
            configuration, and autoscale settings.
        """
        client = get_workspace_client()

        try:
            c = client.clusters.get(cluster_id)
        except Exception as e:
            return json.dumps({"error": f"Failed to get cluster '{cluster_id}': {e}"})

        autoscale = None
        if c.autoscale:
            autoscale = {
                "min_workers": c.autoscale.min_workers,
                "max_workers": c.autoscale.max_workers,
            }

        result: dict[str, Any] = {
            "cluster_id": c.cluster_id,
            "cluster_name": c.cluster_name or "",
            "state": str(c.state) if c.state else "UNKNOWN",
            "state_message": c.state_message or "",
            "creator_user_name": c.creator_user_name or "",
            "spark_version": c.spark_version or "",
            "node_type_id": c.node_type_id or "",
            "driver_node_type_id": c.driver_node_type_id or "",
            "num_workers": c.num_workers if c.num_workers is not None else None,
            "autoscale": autoscale,
            "autotermination_minutes": c.autotermination_minutes or 0,
            "start_time": str(c.start_time) if c.start_time else None,
            "last_activity_time": str(getattr(c, "last_activity_time", None) or "") or None,
            "cluster_source": str(c.cluster_source) if c.cluster_source else "",
            "spark_conf": dict(c.spark_conf) if c.spark_conf else {},
        }

        return json.dumps(result, indent=2)

    @mcp.tool()
    def start_cluster(cluster_id: str, confirm: bool = False) -> str:
        """Start a terminated Databricks cluster.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to start.

        Args:
            cluster_id: The ID of the cluster to start.
            confirm: Set to True to actually start the cluster.

        Returns:
            JSON with start preview or result.
        """
        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would start cluster '{cluster_id}'.",
                "warning": "Set confirm=True to actually start the cluster.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.clusters.start(cluster_id=cluster_id)
            return json.dumps({
                "action": "started",
                "cluster_id": cluster_id,
                "status": "start command sent — cluster is now starting",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to start cluster '{cluster_id}': {e}"})

    @mcp.tool()
    def stop_cluster(cluster_id: str, confirm: bool = False) -> str:
        """Stop (terminate) a running Databricks cluster.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to stop.

        Args:
            cluster_id: The ID of the cluster to stop.
            confirm: Set to True to actually stop the cluster.

        Returns:
            JSON with stop preview or result.
        """
        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would stop cluster '{cluster_id}'.",
                "warning": "Set confirm=True to actually stop the cluster.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.clusters.delete(cluster_id=cluster_id)
            return json.dumps({
                "action": "stopped",
                "cluster_id": cluster_id,
                "status": "stop command sent — cluster is now terminating",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to stop cluster '{cluster_id}': {e}"})

    @mcp.tool()
    def restart_cluster(cluster_id: str, confirm: bool = False) -> str:
        """Restart a running Databricks cluster.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to restart.

        Args:
            cluster_id: The ID of the cluster to restart.
            confirm: Set to True to actually restart the cluster.

        Returns:
            JSON with restart preview or result.
        """
        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would restart cluster '{cluster_id}'.",
                "warning": "Set confirm=True to actually restart the cluster.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.clusters.restart(cluster_id=cluster_id)
            return json.dumps({
                "action": "restarted",
                "cluster_id": cluster_id,
                "status": "restart command sent — cluster is now restarting",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to restart cluster '{cluster_id}': {e}"})
