"""Unit tests for workspace operations tools (create_job, create_notebook, workspace_upload)."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from fastmcp import FastMCP


def _tool_names(mcp: FastMCP) -> list[str]:
    """Get registered tool names from a FastMCP instance."""
    tools = asyncio.run(mcp.list_tools())
    return [t.name for t in tools]


def _call_tool(mcp: FastMCP, name: str, args: dict) -> dict:
    """Call an MCP tool and return parsed JSON result."""
    result = asyncio.run(mcp.call_tool(name, args))
    text = result.content[0].text
    return json.loads(text)


def _register() -> FastMCP:
    """Create an MCP instance with workspace_ops tools registered."""
    from databricks_advanced_mcp.tools.workspace_ops import register

    mcp = FastMCP("test")
    register(mcp)
    return mcp


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


class TestToolRegistration:
    def test_all_tools_registered(self):
        mcp = _register()
        names = _tool_names(mcp)
        assert "create_job" in names
        assert "create_notebook" in names
        assert "workspace_upload" in names
        assert "read_notebook" in names
        assert "delete_workspace_item" in names
        assert "get_workspace_status" in names


# ------------------------------------------------------------------
# create_job
# ------------------------------------------------------------------


class TestCreateJob:
    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_preview_mode(self, mock_get_client):
        """confirm=False returns a preview without calling the API."""
        from databricks_advanced_mcp.tools.workspace_ops import register

        mcp = FastMCP("test")
        register(mcp)

        result = _call_tool(mcp, "create_job", {
            "name": "test-job",
            "notebook_path": "/Workspace/etl/pipeline",
        })

        assert result["action"] == "preview"
        assert "Would create job" in result["message"]
        mock_get_client.assert_not_called()

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_create_job_success(self, mock_get_client):
        """confirm=True creates the job."""
        client = MagicMock()
        mock_get_client.return_value = client

        response = MagicMock()
        response.job_id = 42
        client.jobs.create.return_value = response

        mcp = FastMCP("test")
        from databricks_advanced_mcp.tools.workspace_ops import register
        register(mcp)

        result = _call_tool(mcp, "create_job", {
            "name": "my-etl-job",
            "notebook_path": "/Workspace/etl/pipeline",
            "existing_cluster_id": "0123-456789-abcdef",
            "confirm": True,
        })

        assert result["action"] == "created"
        assert result["job_id"] == "42"
        assert result["status"] == "created"
        client.jobs.create.assert_called_once()

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_create_job_with_schedule(self, mock_get_client):
        """Job creation with cron schedule."""
        client = MagicMock()
        mock_get_client.return_value = client

        response = MagicMock()
        response.job_id = 99
        client.jobs.create.return_value = response

        mcp = FastMCP("test")
        from databricks_advanced_mcp.tools.workspace_ops import register
        register(mcp)

        result = _call_tool(mcp, "create_job", {
            "name": "nightly-job",
            "notebook_path": "/Workspace/etl/nightly",
            "cron_expression": "0 0 0 * * ?",
            "timezone": "UTC",
            "confirm": True,
        })

        assert result["action"] == "created"
        assert result["job_id"] == "99"

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_create_job_api_error(self, mock_get_client):
        """API failure returns error JSON."""
        client = MagicMock()
        mock_get_client.return_value = client
        client.jobs.create.side_effect = Exception("Permission denied")

        mcp = FastMCP("test")
        from databricks_advanced_mcp.tools.workspace_ops import register
        register(mcp)

        result = _call_tool(mcp, "create_job", {
            "name": "bad-job",
            "notebook_path": "/nope",
            "confirm": True,
        })

        assert "error" in result
        assert "Permission denied" in result["error"]


# ------------------------------------------------------------------
# create_notebook
# ------------------------------------------------------------------


class TestCreateNotebook:
    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_preview_mode(self, mock_get_client):
        mcp = _register()

        result = _call_tool(mcp, "create_notebook", {
            "path": "/Workspace/dev/my_notebook",
            "language": "PYTHON",
        })

        assert result["action"] == "preview"
        mock_get_client.assert_not_called()

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_create_empty_notebook(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client

        mcp = _register()

        result = _call_tool(mcp, "create_notebook", {
            "path": "/Workspace/dev/my_notebook",
            "language": "PYTHON",
            "confirm": True,
        })

        assert result["action"] == "created"
        assert result["path"] == "/Workspace/dev/my_notebook"
        assert result["language"] == "PYTHON"
        client.workspace.import_.assert_called_once()

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_create_notebook_with_content(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client

        mcp = _register()

        result = _call_tool(mcp, "create_notebook", {
            "path": "/Workspace/dev/analysis",
            "language": "SQL",
            "content": "SELECT * FROM main.default.events",
            "confirm": True,
        })

        assert result["action"] == "created"
        call_kwargs = client.workspace.import_.call_args
        # Content should be base64-encoded
        assert call_kwargs.kwargs.get("content") or call_kwargs[1].get("content")

    def test_invalid_language(self):
        mcp = _register()

        result = _call_tool(mcp, "create_notebook", {
            "path": "/Workspace/dev/nb",
            "language": "JAVASCRIPT",
        })

        assert "error" in result
        assert "Unsupported language" in result["error"]

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_api_error(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client
        client.workspace.import_.side_effect = Exception("RESOURCE_ALREADY_EXISTS")

        mcp = _register()

        result = _call_tool(mcp, "create_notebook", {
            "path": "/Workspace/dev/existing",
            "language": "PYTHON",
            "confirm": True,
        })

        assert "error" in result


# ------------------------------------------------------------------
# workspace_upload
# ------------------------------------------------------------------


class TestWorkspaceUpload:
    def test_file_not_found(self):
        mcp = _register()

        result = _call_tool(mcp, "workspace_upload", {
            "local_path": "/nonexistent/file.py",
            "workspace_path": "/Workspace/uploads/file.py",
        })

        assert "error" in result
        assert "not found" in result["error"]

    def test_preview_mode(self):
        mcp = _register()

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
            f.write(b"print('hello')")
            tmp_path = f.name

        try:
            result = _call_tool(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": "/Workspace/uploads/test.py",
            })

            assert result["action"] == "preview"
            assert result["upload_config"]["detected_format"] == "SOURCE"
        finally:
            os.unlink(tmp_path)

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_upload_python_file(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client

        mcp = _register()

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
            f.write(b"print('hello')")
            tmp_path = f.name

        try:
            result = _call_tool(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": "/Workspace/uploads/test.py",
                "confirm": True,
            })

            assert result["action"] == "uploaded"
            assert result["format"] == "SOURCE"
            client.workspace.import_.assert_called_once()
        finally:
            os.unlink(tmp_path)

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_upload_ipynb_as_jupyter(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client

        mcp = _register()

        with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as f:
            f.write(b'{"cells":[],"metadata":{}}')
            tmp_path = f.name

        try:
            result = _call_tool(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": "/Workspace/uploads/nb.ipynb",
                "confirm": True,
            })

            assert result["action"] == "uploaded"
            assert result["format"] == "JUPYTER"
        finally:
            os.unlink(tmp_path)

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_upload_unknown_extension(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client

        mcp = _register()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"key": "value"}')
            tmp_path = f.name

        try:
            result = _call_tool(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": "/Workspace/uploads/config.json",
                "confirm": True,
            })

            assert result["action"] == "uploaded"
            assert result["format"] == "AUTO"
        finally:
            os.unlink(tmp_path)

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_upload_api_error(self, mock_get_client):
        client = MagicMock()
        mock_get_client.return_value = client
        client.workspace.import_.side_effect = Exception("QUOTA_EXCEEDED")

        mcp = _register()

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
            f.write(b"data")
            tmp_path = f.name

        try:
            result = _call_tool(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": "/Workspace/uploads/test.py",
                "confirm": True,
            })

            assert "error" in result
            assert "QUOTA_EXCEEDED" in result["error"]
        finally:
            os.unlink(tmp_path)
