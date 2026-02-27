"""Dependency scanner MCP tools.

Provides tools for scanning notebooks, jobs, DLT pipelines, and building
the workspace dependency graph.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client
from databricks_advanced_mcp.config import get_settings
from databricks_advanced_mcp.graph.builder import GraphBuilder
from databricks_advanced_mcp.graph.cache import GraphCache
from databricks_advanced_mcp.graph.models import NodeType
from databricks_advanced_mcp.parsers.notebook_parser import parse_notebook


def register(mcp: FastMCP) -> None:
    """Register dependency scanner tools with the MCP server."""

    @mcp.tool()
    def scan_notebook(notebook_path: str) -> str:
        """Scan a Databricks notebook for table references.

        Fetches the notebook source via Workspace API, parses SQL cells
        and PySpark code, and returns all table references found.

        Args:
            notebook_path: Workspace path to the notebook (e.g. /Workspace/Users/me/my_notebook).

        Returns:
            JSON with table references found in the notebook.
        """
        import base64

        client = get_workspace_client()

        try:
            export = client.workspace.export(notebook_path)
        except Exception as e:
            return json.dumps({"error": f"Failed to export notebook: {e}"})

        source = export.content or ""
        if source:
            try:
                source = base64.b64decode(source).decode("utf-8")
            except Exception:
                pass

        language = "python"  # Default; could detect from notebook metadata
        result = parse_notebook(source, default_language=language)

        refs = [
            {
                "table": ref.fqn,
                "reference_type": ref.reference_type,
                "catalog": ref.catalog,
                "schema": ref.schema,
                "table_name": ref.table,
            }
            for ref in result.table_references
        ]

        return json.dumps({
            "notebook_path": notebook_path,
            "cell_count": len(result.cells),
            "table_reference_count": len(refs),
            "table_references": refs,
        }, indent=2)

    @mcp.tool()
    def scan_jobs() -> str:
        """Scan all Databricks jobs to extract table dependencies.

        Lists all jobs via the Jobs API, inspects task definitions
        (notebook tasks, SQL tasks, DLT pipeline tasks), and extracts
        table references from associated notebooks and SQL statements.

        Returns:
            JSON with job-level dependency summary.
        """
        client = get_workspace_client()

        try:
            jobs = list(client.jobs.list())
        except Exception as e:
            return json.dumps({"error": f"Failed to list jobs: {e}"})

        results: list[dict[str, Any]] = []
        for job in jobs:
            job_id = str(job.job_id)
            job_name = job.settings.name if job.settings else f"job_{job_id}"

            tasks_info: list[dict[str, Any]] = []
            if job.settings and job.settings.tasks:
                for task in job.settings.tasks:
                    task_info: dict[str, Any] = {
                        "task_key": task.task_key,
                        "type": "unknown",
                    }
                    if task.notebook_task:
                        task_info["type"] = "notebook"
                        task_info["notebook_path"] = task.notebook_task.notebook_path
                    elif task.sql_task:
                        task_info["type"] = "sql"
                    elif task.pipeline_task:
                        task_info["type"] = "pipeline"
                        task_info["pipeline_id"] = task.pipeline_task.pipeline_id
                    tasks_info.append(task_info)

            results.append({
                "job_id": job_id,
                "job_name": job_name,
                "task_count": len(tasks_info),
                "tasks": tasks_info,
            })

        return json.dumps({
            "job_count": len(results),
            "jobs": results,
        }, indent=2)

    @mcp.tool()
    def scan_dlt_pipelines() -> str:
        """Scan all DLT pipelines to extract table dependencies.

        Retrieves pipeline definitions via the Pipelines API and extracts
        source/target table references from configuration and associated notebooks.

        Returns:
            JSON with pipeline dependency information.
        """
        from databricks_advanced_mcp.parsers.dlt_parser import parse_dlt_pipeline_config

        client = get_workspace_client()

        try:
            pipelines = list(client.pipelines.list_pipelines())
        except Exception as e:
            return json.dumps({"error": f"Failed to list pipelines: {e}"})

        results: list[dict[str, Any]] = []
        for ps in pipelines:
            pipeline_id = ps.pipeline_id
            if not pipeline_id:
                continue

            try:
                detail = client.pipelines.get(pipeline_id)
            except Exception as e:
                results.append({
                    "pipeline_id": pipeline_id,
                    "name": ps.name or "",
                    "error": str(e),
                })
                continue

            spec = detail.spec
            config: dict[str, Any] = {
                "pipeline_id": pipeline_id,
                "name": (spec.name if spec else ps.name) or "",
                "target": spec.target if spec else None,
                "catalog": spec.catalog if spec else None,
                "libraries": [],
            }
            if spec and spec.libraries:
                for lib in spec.libraries:
                    if lib.notebook:
                        config["libraries"].append(
                            {"notebook": {"path": lib.notebook.path}}
                        )

            info = parse_dlt_pipeline_config(config)

            results.append({
                "pipeline_id": pipeline_id,
                "name": info.name,
                "target_catalog": info.target_catalog,
                "target_schema": info.target_schema,
                "notebook_paths": info.notebook_paths,
            })

        return json.dumps({
            "pipeline_count": len(results),
            "pipelines": results,
        }, indent=2)

    @mcp.tool()
    def scan_dlt_pipeline(pipeline_id: str) -> str:
        """Scan a single DLT pipeline to extract table dependencies.

        Retrieves the pipeline definition by ID and extracts source/target
        table references from configuration and associated notebooks.

        Args:
            pipeline_id: The ID of the DLT pipeline to scan.

        Returns:
            JSON with pipeline dependency information.
        """
        from databricks_advanced_mcp.parsers.dlt_parser import parse_dlt_pipeline_config

        client = get_workspace_client()

        try:
            detail = client.pipelines.get(pipeline_id)
        except Exception as e:
            return json.dumps({"error": f"Pipeline not found or inaccessible: {e}"})

        spec = detail.spec
        config: dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "name": (spec.name if spec else "") or "",
            "target": spec.target if spec else None,
            "catalog": spec.catalog if spec else None,
            "libraries": [],
        }
        if spec and spec.libraries:
            for lib in spec.libraries:
                if lib.notebook:
                    config["libraries"].append(
                        {"notebook": {"path": lib.notebook.path}}
                    )

        info = parse_dlt_pipeline_config(config)

        return json.dumps({
            "pipeline_id": pipeline_id,
            "name": info.name,
            "target_catalog": info.target_catalog,
            "target_schema": info.target_schema,
            "notebook_paths": info.notebook_paths,
        }, indent=2)

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
