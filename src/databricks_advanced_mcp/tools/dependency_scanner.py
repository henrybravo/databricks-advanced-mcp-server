"""Dependency scanner MCP tools.

Provides tools for scanning notebooks, jobs, and DLT pipelines to
extract table references and task definitions.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client
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
            with contextlib.suppress(Exception):
                source = base64.b64decode(source).decode("utf-8")

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

            # Fetch full job details — list() may omit task definitions
            settings = job.settings
            try:
                job_detail = client.jobs.get(int(job_id))
                if job_detail.settings:
                    settings = job_detail.settings
            except Exception:
                pass  # Fall back to list() data

            job_name = settings.name if settings else f"job_{job_id}"

            tasks_info: list[dict[str, Any]] = []
            raw_tasks = (settings.tasks if settings else None) or []
            for task in raw_tasks:
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

