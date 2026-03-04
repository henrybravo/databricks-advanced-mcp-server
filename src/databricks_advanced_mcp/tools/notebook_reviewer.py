"""Notebook reviewer MCP tool.

Fetches a notebook and runs all review rule sets (performance, standards,
optimization suggestions), returning structured findings.
"""

from __future__ import annotations

import base64
import contextlib
import json
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client
from databricks_advanced_mcp.parsers.notebook_parser import CellType, parse_notebook
from databricks_advanced_mcp.reviewers.performance import ReviewFinding, check_performance
from databricks_advanced_mcp.reviewers.standards import check_standards
from databricks_advanced_mcp.reviewers.suggestions import check_suggestions


def register(mcp: FastMCP) -> None:
    """Register notebook reviewer tools with the MCP server."""

    @mcp.tool()
    def review_notebook(
        notebook_path: str,
        categories: str = "all",
    ) -> str:
        """Review a Databricks notebook for issues and optimization opportunities.

        Fetches the notebook source, analyzes each code cell against
        performance rules, coding standards, and optimization suggestions,
        then returns structured findings.

        Args:
            notebook_path: Workspace path to the notebook.
            categories: Comma-separated categories to check: "performance", "standards",
                       "optimization", or "all" (default).

        Returns:
            JSON review report with findings grouped by severity.
        """
        client = get_workspace_client()

        # Fetch notebook source
        try:
            export = client.workspace.export(notebook_path)
        except Exception as e:
            return json.dumps({"error": f"Failed to export notebook: {e}"})

        source = export.content or ""
        if source:
            with contextlib.suppress(Exception):
                source = base64.b64decode(source).decode("utf-8")

        # Parse notebook into cells
        result = parse_notebook(source)

        # Determine which categories to run
        if categories == "all":
            run_categories = {"performance", "standards", "optimization"}
        else:
            run_categories = {c.strip().lower() for c in categories.split(",")}

        # Run review rules on each cell
        all_findings: list[ReviewFinding] = []

        for cell in result.cells:
            if cell.cell_type == CellType.MARKDOWN:
                continue

            cell_type = cell.cell_type.value

            if "performance" in run_categories:
                all_findings.extend(check_performance(cell.content, cell.index, cell_type))

            if "standards" in run_categories:
                all_findings.extend(check_standards(cell.content, cell.index, cell_type))

            if "optimization" in run_categories:
                all_findings.extend(check_suggestions(cell.content, cell.index, cell_type))

        # Build report
        severity_counts: dict[str, int] = {}
        for f in all_findings:
            severity_counts[f.severity] = severity_counts.get(f.severity, 0) + 1

        findings_list: list[dict[str, Any]] = [
            {
                "rule_id": f.rule_id,
                "category": f.category,
                "severity": f.severity,
                "cell_index": f.cell_index,
                "message": f.message,
                "suggestion": f.suggestion,
                "code_snippet": f.code_snippet,
            }
            for f in sorted(
                all_findings,
                key=lambda x: {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}.get(x.severity, 5),
            )
        ]

        report: dict[str, Any] = {
            "notebook_path": notebook_path,
            "cell_count": len(result.cells),
            "total_findings": len(all_findings),
            "severity_counts": severity_counts,
            "categories_checked": sorted(run_categories),
            "findings": findings_list,
        }

        return json.dumps(report, indent=2)
