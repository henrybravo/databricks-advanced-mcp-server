"""Impact analysis MCP tools.

Provides tools for analyzing the impact of schema changes, column drops,
and pipeline failures across the dependency graph.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.graph.cache import GraphCache
from databricks_advanced_mcp.graph.models import DependencyGraph, NodeType

# ------------------------------------------------------------------
# Impact report data structures
# ------------------------------------------------------------------

@dataclass
class AffectedAsset:
    """An asset affected by a change."""

    node_id: str
    node_type: str
    name: str
    fqn: str
    severity: str  # critical, high, medium, low
    relationship_path: list[str] = field(default_factory=list)
    reason: str = ""


@dataclass
class ImpactReport:
    """Structured impact analysis report."""

    change_description: str
    change_type: str
    affected_assets: list[AffectedAsset] = field(default_factory=list)
    total_affected_count: int = 0
    severity_counts: dict[str, int] = field(default_factory=dict)
    risk_score: float = 0.0  # 0-10 scale
    graph_timestamp: float = 0.0
    stale_warning: bool = False
    remediation: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for JSON output."""
        return {
            "change_description": self.change_description,
            "change_type": self.change_type,
            "total_affected_count": self.total_affected_count,
            "severity_counts": self.severity_counts,
            "risk_score": round(self.risk_score, 1),
            "graph_timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.graph_timestamp)
            ) if self.graph_timestamp else None,
            "stale_warning": self.stale_warning,
            "affected_assets": [
                {
                    "node_id": a.node_id,
                    "type": a.node_type,
                    "name": a.name,
                    "fqn": a.fqn,
                    "severity": a.severity,
                    "relationship_path": a.relationship_path,
                    "reason": a.reason,
                }
                for a in self.affected_assets
            ],
            "remediation": self.remediation,
        }


# ------------------------------------------------------------------
# Severity classification
# ------------------------------------------------------------------

_SEVERITY_WEIGHTS = {"critical": 10, "high": 7, "medium": 4, "low": 1}


def _classify_severity(node_type: str, depth: int) -> str:
    """Classify severity based on asset type and distance from change."""
    if depth <= 1:
        if node_type in ("job", "pipeline"):
            return "critical"
        return "high"
    elif depth <= 3:
        if node_type in ("job", "pipeline"):
            return "high"
        return "medium"
    return "low"


def _compute_risk_score(assets: list[AffectedAsset]) -> float:
    """Compute an overall risk score (0-10) from affected assets."""
    if not assets:
        return 0.0
    total_weight = sum(_SEVERITY_WEIGHTS.get(a.severity, 1) for a in assets)
    # Normalize: max 10, scale by count
    score = min(10.0, total_weight / max(len(assets), 1) * min(len(assets), 10) / 10 * 10)
    return score


# ------------------------------------------------------------------
# Analysis functions
# ------------------------------------------------------------------

def _get_graph_or_error() -> tuple[DependencyGraph | None, GraphCache, dict[str, Any] | None]:
    """Get the cached graph or return an error dict.

    Uses get_or_stale() so that a stale graph is still served with a warning
    rather than returning an error.  Returns None only when no graph exists.
    """
    cache = GraphCache.get_instance()
    graph = cache.get_or_stale()
    if graph is None:
        return None, cache, {
            "error": "Dependency graph not built yet. Run build_dependency_graph first.",
            "cache_status": cache.summary(),
        }
    return graph, cache, None


def _build_affected_assets(
    graph: DependencyGraph,
    start_node_id: str,
    direction: str = "downstream",
) -> list[AffectedAsset]:
    """Traverse the graph and build a list of affected assets with depth info."""
    related = graph.get_downstream(start_node_id) if direction == "downstream" else graph.get_upstream(start_node_id)

    assets: list[AffectedAsset] = []
    for node_id in related:
        node_data = graph.get_node(node_id)
        if not node_data:
            continue

        # Compute path for depth
        if direction == "downstream":
            path = graph.get_path(start_node_id, node_id)
        else:
            path = graph.get_path(node_id, start_node_id)
        depth = len(path) - 1 if path else 1

        node_type = node_data.get("node_type", "unknown")
        severity = _classify_severity(node_type, depth)

        assets.append(AffectedAsset(
            node_id=node_id,
            node_type=node_type,
            name=node_data.get("name", ""),
            fqn=node_data.get("fqn", ""),
            severity=severity,
            relationship_path=path,
        ))

    return assets


def analyze_column_drop(
    table_name: str,
    column_name: str,
    graph: DependencyGraph,
    cache: GraphCache,
) -> ImpactReport:
    """Analyze impact of dropping a column from a table."""
    table_id = f"{NodeType.TABLE.value}::{table_name}"
    node = graph.get_node(table_id)

    report = ImpactReport(
        change_description=f"Drop column '{column_name}' from table '{table_name}'",
        change_type="column_drop",
        graph_timestamp=cache.timestamp,
        stale_warning=cache.is_stale(),
    )

    if node is None:
        report.remediation.append(
            f"Table '{table_name}' not found in graph. Consider rebuilding the graph."
        )
        return report

    assets = _build_affected_assets(graph, table_id, direction="downstream")

    # Annotate reason
    for asset in assets:
        asset.reason = f"May reference column '{column_name}' from '{table_name}'"

    report.affected_assets = assets
    report.total_affected_count = len(assets)
    report.severity_counts = _count_severities(assets)
    report.risk_score = _compute_risk_score(assets)

    # Remediation suggestions
    if assets:
        report.remediation.extend([
            f"Check all {len(assets)} downstream assets for references to column '{column_name}'.",
            "Consider adding a deprecation period before removing the column.",
            "Update downstream queries/notebooks to remove or replace the column reference.",
        ])

    return report


def analyze_schema_change(
    table_name: str,
    changes: list[dict[str, Any]],
    graph: DependencyGraph,
    cache: GraphCache,
) -> ImpactReport:
    """Analyze impact of schema modifications on a table.

    Args:
        table_name: The table being modified.
        changes: List of changes, each with 'action' (add/remove/modify),
                 'column', and optionally 'new_type'.
        graph: The dependency graph.
        cache: The graph cache.
    """
    change_desc_parts = []
    for c in changes:
        action = c.get("action", "modify")
        col = c.get("column", "?")
        change_desc_parts.append(f"{action} column '{col}'")

    report = ImpactReport(
        change_description=f"Schema change on '{table_name}': {', '.join(change_desc_parts)}",
        change_type="schema_change",
        graph_timestamp=cache.timestamp,
        stale_warning=cache.is_stale(),
    )

    table_id = f"{NodeType.TABLE.value}::{table_name}"
    if graph.get_node(table_id) is None:
        report.remediation.append(
            f"Table '{table_name}' not found in graph."
        )
        return report

    assets = _build_affected_assets(graph, table_id, direction="downstream")

    # Classify impact per change type
    removed = [c for c in changes if c.get("action") == "remove"]
    modified = [c for c in changes if c.get("action") == "modify"]

    for asset in assets:
        reasons = []
        if removed:
            cols = ", ".join(c.get("column", "?") for c in removed)
            reasons.append(f"Removed columns: {cols}")
        if modified:
            cols = ", ".join(c.get("column", "?") for c in modified)
            reasons.append(f"Type-changed columns: {cols}")
        asset.reason = "; ".join(reasons) if reasons else "May be affected by schema change"

    # Elevate severity for removals
    if removed:
        for asset in assets:
            if asset.severity == "medium":
                asset.severity = "high"
            elif asset.severity == "low":
                asset.severity = "medium"

    report.affected_assets = assets
    report.total_affected_count = len(assets)
    report.severity_counts = _count_severities(assets)
    report.risk_score = _compute_risk_score(assets)

    if removed:
        report.remediation.append(
            "Column removals have highest impact. Consider deprecation-first approach."
        )
    if modified:
        report.remediation.append(
            "Type changes may cause implicit cast failures. Validate downstream consumers."
        )

    return report


def analyze_pipeline_failure(
    pipeline_id: str,
    graph: DependencyGraph,
    cache: GraphCache,
) -> ImpactReport:
    """Analyze downstream impact if a pipeline/job fails."""
    # Try pipeline first, then job
    node_id = f"{NodeType.PIPELINE.value}::{pipeline_id}"
    node = graph.get_node(node_id)

    if node is None:
        node_id = f"{NodeType.JOB.value}::{pipeline_id}"
        node = graph.get_node(node_id)

    report = ImpactReport(
        change_description=f"Failure of pipeline/job '{pipeline_id}'",
        change_type="pipeline_failure",
        graph_timestamp=cache.timestamp,
        stale_warning=cache.is_stale(),
    )

    if node is None:
        report.remediation.append(
            f"Pipeline/job '{pipeline_id}' not found in graph."
        )
        return report

    assets = _build_affected_assets(graph, node_id, direction="downstream")

    for asset in assets:
        asset.reason = f"Depends on output of '{pipeline_id}' (may receive stale/missing data)"

    report.affected_assets = assets
    report.total_affected_count = len(assets)
    report.severity_counts = _count_severities(assets)
    report.risk_score = _compute_risk_score(assets)

    if assets:
        report.remediation.extend([
            f"Monitor {len(assets)} downstream assets for data freshness.",
            "Consider setting up alerting for cascading failures.",
            "Review retry/recovery configuration for the failed pipeline.",
        ])

    return report


def _count_severities(assets: list[AffectedAsset]) -> dict[str, int]:
    """Count assets by severity level."""
    counts: dict[str, int] = {}
    for a in assets:
        counts[a.severity] = counts.get(a.severity, 0) + 1
    return counts


# ------------------------------------------------------------------
# MCP Tool registration
# ------------------------------------------------------------------

def register(mcp: FastMCP) -> None:
    """Register impact analysis tools with the MCP server."""

    @mcp.tool()
    def analyze_impact(
        change_type: str,
        table_name: str = "",
        column_name: str = "",
        schema_changes: str = "",
        pipeline_id: str = "",
    ) -> str:
        """Analyze the impact of a proposed change on downstream assets.

        Traverses the dependency graph to identify all affected assets
        and generates a structured impact report with severity levels
        and remediation suggestions.

        Args:
            change_type: Type of change — "column_drop", "schema_change", or "pipeline_failure".
            table_name: Target table (required for column_drop and schema_change).
            column_name: Column being dropped (required for column_drop).
            schema_changes: JSON array of changes for schema_change, e.g. [{"action":"remove","column":"col1"}].
            pipeline_id: Pipeline or job ID (required for pipeline_failure).

        Returns:
            JSON impact report with affected assets, severity, risk score, and remediation.
        """
        graph, cache, error = _get_graph_or_error()
        if error:
            return json.dumps(error)

        assert graph is not None  # for type checker

        if change_type == "column_drop":
            if not table_name or not column_name:
                return json.dumps({
                    "error": "column_drop requires both table_name and column_name."
                })
            report = analyze_column_drop(table_name, column_name, graph, cache)

        elif change_type == "schema_change":
            if not table_name or not schema_changes:
                return json.dumps({
                    "error": "schema_change requires table_name and schema_changes JSON."
                })
            try:
                changes = json.loads(schema_changes)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid schema_changes JSON: {e}"})
            report = analyze_schema_change(table_name, changes, graph, cache)

        elif change_type == "pipeline_failure":
            if not pipeline_id:
                return json.dumps({
                    "error": "pipeline_failure requires pipeline_id."
                })
            report = analyze_pipeline_failure(pipeline_id, graph, cache)

        else:
            return json.dumps({
                "error": f"Unknown change_type: '{change_type}'. Use column_drop, schema_change, or pipeline_failure."
            })

        return json.dumps(report.to_dict(), indent=2)
