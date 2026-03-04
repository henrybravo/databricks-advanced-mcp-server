"""Workspace operations MCP tools.

Provides tools for creating jobs, creating notebooks, and uploading
files to the Databricks workspace.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client

_VALID_LANGUAGES = {"PYTHON", "SQL", "SCALA", "R"}

# Map file extensions to Databricks import formats
_NOTEBOOK_EXTENSIONS: dict[str, tuple[str, str]] = {
    ".py": ("SOURCE", "PYTHON"),
    ".sql": ("SOURCE", "SQL"),
    ".scala": ("SOURCE", "SCALA"),
    ".r": ("SOURCE", "R"),
    ".ipynb": ("JUPYTER", "PYTHON"),
}


def register(mcp: FastMCP) -> None:
    """Register workspace operations tools."""

    @mcp.tool()
    def create_job(
        name: str,
        notebook_path: str,
        existing_cluster_id: str = "",
        cron_expression: str = "",
        timezone: str = "UTC",
        confirm: bool = False,
    ) -> str:
        """Create a new Databricks job with a notebook task.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview of what would be created. Set confirm=True
        to actually create the job.

        Args:
            name: Name for the new job.
            notebook_path: Workspace path to the notebook to run.
            existing_cluster_id: ID of an existing cluster to use.
                If empty, the job will use a new job cluster (serverless or default).
            cron_expression: Optional Quartz cron expression for scheduling
                (e.g. "0 0 0 * * ?" for daily at midnight).
            timezone: Timezone for the cron schedule (default: "UTC").
            confirm: Set to True to actually create the job.

        Returns:
            JSON with job preview or creation result.
        """
        from databricks.sdk.service.jobs import (
            CronSchedule,
            NotebookTask,
            Task,
        )

        task = Task(
            task_key="main_task",
            notebook_task=NotebookTask(notebook_path=notebook_path),
        )
        if existing_cluster_id:
            task.existing_cluster_id = existing_cluster_id

        schedule = None
        if cron_expression:
            schedule = CronSchedule(
                quartz_cron_expression=cron_expression,
                timezone_id=timezone,
            )

        preview: dict[str, Any] = {
            "name": name,
            "notebook_path": notebook_path,
            "existing_cluster_id": existing_cluster_id or None,
            "schedule": {
                "cron": cron_expression,
                "timezone": timezone,
            } if cron_expression else None,
        }

        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would create job '{name}' with notebook task '{notebook_path}'.",
                "job_config": preview,
                "warning": "Set confirm=True to actually create the job.",
            }, indent=2)

        client = get_workspace_client()

        create_kwargs: dict[str, Any] = {
            "name": name,
            "tasks": [task],
        }
        if schedule:
            create_kwargs["schedule"] = schedule

        try:
            response = client.jobs.create(**create_kwargs)
            return json.dumps({
                "action": "created",
                "job_id": str(response.job_id),
                "name": name,
                "status": "created",
                "job_config": preview,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to create job: {e}"})

    @mcp.tool()
    def create_notebook(
        path: str,
        language: str = "PYTHON",
        content: str = "",
        overwrite: bool = False,
        confirm: bool = False,
    ) -> str:
        """Create a new notebook in the Databricks workspace.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview of what would be created. Set confirm=True
        to actually create the notebook.

        Args:
            path: Workspace path for the new notebook (e.g. "/Workspace/dev/my_notebook").
            language: Notebook language: PYTHON, SQL, SCALA, or R.
            content: Optional initial content for the notebook.
            overwrite: Whether to overwrite if a notebook exists at the path (default: False).
            confirm: Set to True to actually create the notebook.

        Returns:
            JSON with notebook preview or creation result.
        """
        from databricks.sdk.service.workspace import ImportFormat, Language

        lang_upper = language.upper()
        if lang_upper not in _VALID_LANGUAGES:
            return json.dumps({
                "error": f"Unsupported language: {language}. Supported: {', '.join(sorted(_VALID_LANGUAGES))}",
            })

        preview: dict[str, Any] = {
            "path": path,
            "language": lang_upper,
            "has_content": bool(content),
            "overwrite": overwrite,
        }

        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would create {lang_upper} notebook at '{path}'.",
                "notebook_config": preview,
                "warning": "Set confirm=True to actually create the notebook.",
            }, indent=2)

        client = get_workspace_client()

        language_enum = Language[lang_upper]
        encoded_content = base64.b64encode(content.encode("utf-8")).decode("ascii") if content else None

        try:
            # Ensure parent directory exists
            parent = path.rsplit("/", 1)[0]
            if parent:
                client.workspace.mkdirs(parent)

            client.workspace.import_(
                path=path,
                format=ImportFormat.SOURCE,
                language=language_enum,
                content=encoded_content,
                overwrite=overwrite,
            )
            return json.dumps({
                "action": "created",
                "path": path,
                "language": lang_upper,
                "status": "created",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to create notebook: {e}"})

    @mcp.tool()
    def workspace_upload(
        local_path: str,
        workspace_path: str,
        overwrite: bool = False,
        confirm: bool = False,
    ) -> str:
        """Upload a local file to a Databricks workspace path.

        Auto-detects notebook format from file extension (.py, .sql, .scala,
        .r → SOURCE; .ipynb → JUPYTER; others → AUTO).

        This is a MUTATING operation. When confirm=False (default),
        returns a preview. Set confirm=True to upload.

        Args:
            local_path: Path to the local file to upload.
            workspace_path: Target workspace path (e.g. "/Workspace/uploads/etl.py").
            overwrite: Whether to overwrite if an object exists at the path (default: False).
            confirm: Set to True to actually upload the file.

        Returns:
            JSON with upload preview or result.
        """
        from databricks.sdk.service.workspace import ImportFormat

        if not os.path.isfile(local_path):
            return json.dumps({"error": f"Local file not found: {local_path}"})

        ext = os.path.splitext(local_path)[1].lower()
        ext_info = _NOTEBOOK_EXTENSIONS.get(ext)
        format_name = ext_info[0] if ext_info else "AUTO"
        language_name = ext_info[1] if ext_info else None

        file_size = os.path.getsize(local_path)

        preview: dict[str, Any] = {
            "local_path": local_path,
            "workspace_path": workspace_path,
            "file_size_bytes": file_size,
            "detected_format": format_name,
            "detected_language": language_name,
            "overwrite": overwrite,
        }

        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would upload '{local_path}' to '{workspace_path}' as {format_name} format.",
                "upload_config": preview,
                "warning": "Set confirm=True to actually upload.",
            }, indent=2)

        from databricks.sdk.service.workspace import Language as WsLanguage

        with open(local_path, "rb") as f:
            raw_content = f.read()

        encoded_content = base64.b64encode(raw_content).decode("ascii")
        import_format = ImportFormat[format_name]

        client = get_workspace_client()

        # Ensure parent directory exists
        parent = workspace_path.rsplit("/", 1)[0]
        if parent:
            client.workspace.mkdirs(parent)

        import_kwargs: dict[str, Any] = {
            "path": workspace_path,
            "format": import_format,
            "content": encoded_content,
            "overwrite": overwrite,
        }
        if language_name:
            import_kwargs["language"] = WsLanguage[language_name]

        try:
            client.workspace.import_(**import_kwargs)
            return json.dumps({
                "action": "uploaded",
                "workspace_path": workspace_path,
                "status": "uploaded",
                "file_size_bytes": file_size,
                "format": format_name,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to upload file: {e}"})

    @mcp.tool()
    def read_notebook(
        notebook_path: str,
        format: str = "SOURCE",
    ) -> str:
        """Read/export the content of a Databricks notebook.

        Args:
            notebook_path: Workspace path to the notebook
                           (e.g. "/Workspace/Users/me/my_notebook").
            format: Export format — "SOURCE" (default) or "HTML".

        Returns:
            JSON with notebook content and metadata.
        """
        import base64 as b64

        from databricks.sdk.service.workspace import ExportFormat

        valid_formats = {"SOURCE", "HTML"}
        fmt_upper = format.upper()
        if fmt_upper not in valid_formats:
            return json.dumps({
                "error": f"Invalid format '{format}'. Use one of: {', '.join(sorted(valid_formats))}."
            })

        client = get_workspace_client()

        try:
            export = client.workspace.export(
                notebook_path,
                format=ExportFormat[fmt_upper],
            )
        except Exception as e:
            return json.dumps({"error": f"Failed to export notebook: {e}"})

        content = export.content or ""
        if content:
            try:
                content = b64.b64decode(content).decode("utf-8")
            except Exception:
                pass  # Return raw if decode fails

        return json.dumps({
            "notebook_path": notebook_path,
            "format": fmt_upper,
            "content_length": len(content),
            "content": content,
        }, indent=2)

    @mcp.tool()
    def delete_workspace_item(
        path: str,
        recursive: bool = False,
        confirm: bool = False,
    ) -> str:
        """Delete a notebook, file, or folder from the Databricks workspace.

        This is a DESTRUCTIVE operation. When confirm=False (default),
        returns a preview. Set confirm=True to delete.

        Args:
            path: Workspace path to the object to delete.
            recursive: If True and path is a directory, delete all contents
                       recursively. Required for non-empty directories.
            confirm: Set to True to actually delete.

        Returns:
            JSON with deletion preview or result.
        """
        if not confirm:
            return json.dumps({
                "action": "preview",
                "message": f"Would DELETE workspace item at '{path}'"
                           + (" (recursive)" if recursive else "") + ".",
                "delete_config": {
                    "path": path,
                    "recursive": recursive,
                },
                "warning": "⚠ DESTRUCTIVE: Set confirm=True to actually delete.",
            }, indent=2)

        client = get_workspace_client()

        try:
            client.workspace.delete(path, recursive=recursive)
            return json.dumps({
                "action": "deleted",
                "path": path,
                "recursive": recursive,
                "status": "workspace item deleted",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to delete '{path}': {e}"})

    @mcp.tool()
    def get_workspace_status(path: str) -> str:
        """Get metadata for a workspace object (notebook, directory, file, etc.).

        Args:
            path: Workspace path to the object (e.g. "/Workspace/Users/me/my_notebook").

        Returns:
            JSON with object type, language, path, and modification time.
        """
        client = get_workspace_client()

        try:
            status = client.workspace.get_status(path)
        except Exception as e:
            return json.dumps({"error": f"Failed to get status for '{path}': {e}"})

        result: dict[str, Any] = {
            "path": status.path or path,
            "object_type": str(status.object_type) if status.object_type else "UNKNOWN",
            "object_id": status.object_id,
            "language": str(status.language) if status.language else None,
            "created_at": str(status.created_at) if status.created_at else None,
            "modified_at": str(status.modified_at) if status.modified_at else None,
            "size": status.size if status.size else None,
        }

        return json.dumps(result, indent=2)
