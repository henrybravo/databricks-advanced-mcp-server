"""Unit tests for workspace operations tools (create_job, create_notebook, workspace_upload)."""

from __future__ import annotations

import asyncio
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


# ------------------------------------------------------------------
# read_notebook — JUPYTER format conversion
# ------------------------------------------------------------------

# Sample Databricks SOURCE exports used in tests
_PYTHON_SOURCE = """\
# Databricks notebook source

# COMMAND ----------

# Import libraries
import pandas as pd
import numpy as np

# COMMAND ----------

%md
# Analysis Notebook
This notebook analyses sales data.

# COMMAND ----------

%sql
SELECT * FROM main.default.sales ORDER BY date

# COMMAND ----------

# Process data
df = spark.table("main.default.sales")
result = df.groupBy("region").count()
display(result)
"""

_SQL_SOURCE = """\
-- Databricks notebook source
-- Create a sample table

CREATE TABLE IF NOT EXISTS main.default.demo (
  id BIGINT,
  name STRING
);

-- COMMAND ----------

-- Insert data
INSERT INTO main.default.demo VALUES (1, 'Alice'), (2, 'Bob');

-- COMMAND ----------

-- Query data
SELECT * FROM main.default.demo ORDER BY id;
"""


class TestReadNotebookJupyter:
    """Tests for read_notebook with format='JUPYTER'."""

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_jupyter_format_returns_valid_ipynb(self, mock_get_client):
        """JUPYTER format returns a valid .ipynb JSON structure."""
        import base64

        client = MagicMock()
        mock_get_client.return_value = client

        export_mock = MagicMock()
        export_mock.content = base64.b64encode(_PYTHON_SOURCE.encode()).decode()
        client.workspace.export.return_value = export_mock

        status_mock = MagicMock()
        status_mock.language = "PYTHON"
        client.workspace.get_status.return_value = status_mock

        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/Workspace/Users/me/analysis",
            "format": "JUPYTER",
        })

        assert result["format"] == "JUPYTER"
        assert result["notebook_path"] == "/Workspace/Users/me/analysis"

        # Parse the returned content as a notebook
        nb = json.loads(result["content"])
        assert nb["nbformat"] == 4
        assert "cells" in nb
        assert len(nb["cells"]) > 0

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_jupyter_python_cells_split_correctly(self, mock_get_client):
        """Python notebook cells are properly split and typed."""
        import base64

        client = MagicMock()
        mock_get_client.return_value = client

        export_mock = MagicMock()
        export_mock.content = base64.b64encode(_PYTHON_SOURCE.encode()).decode()
        client.workspace.export.return_value = export_mock

        status_mock = MagicMock()
        status_mock.language = "PYTHON"
        client.workspace.get_status.return_value = status_mock

        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/Workspace/Users/me/analysis",
            "format": "JUPYTER",
        })

        nb = json.loads(result["content"])
        cells = nb["cells"]

        # Should have 4 cells: import code, markdown, sql magic, process code
        assert len(cells) == 4

        # First cell is code (import)
        assert cells[0]["cell_type"] == "code"
        source_0 = "".join(cells[0]["source"])
        assert "import pandas" in source_0

        # Second cell is markdown
        assert cells[1]["cell_type"] == "markdown"
        source_1 = "".join(cells[1]["source"])
        assert "Analysis Notebook" in source_1

        # Third cell is code (SQL magic)
        assert cells[2]["cell_type"] == "code"
        source_2 = "".join(cells[2]["source"])
        assert "SELECT" in source_2

        # Fourth cell is code (process data)
        assert cells[3]["cell_type"] == "code"
        source_3 = "".join(cells[3]["source"])
        assert "spark.table" in source_3

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_jupyter_sql_notebook(self, mock_get_client):
        """SQL notebook preserves cells and sets SQL kernel."""
        import base64

        client = MagicMock()
        mock_get_client.return_value = client

        export_mock = MagicMock()
        export_mock.content = base64.b64encode(_SQL_SOURCE.encode()).decode()
        client.workspace.export.return_value = export_mock

        status_mock = MagicMock()
        status_mock.language = "SQL"
        client.workspace.get_status.return_value = status_mock

        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/Workspace/Users/me/sql_demo",
            "format": "JUPYTER",
        })

        nb = json.loads(result["content"])

        # Kernel should be SQL
        assert nb["metadata"]["kernelspec"]["language"] == "sql"
        assert nb["metadata"]["language_info"]["name"] == "sql"

        # Should have 3 code cells
        assert len(nb["cells"]) == 3
        assert all(c["cell_type"] == "code" for c in nb["cells"])

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_jupyter_fetches_source_from_api(self, mock_get_client):
        """JUPYTER format internally fetches SOURCE format from the API."""
        import base64

        from databricks.sdk.service.workspace import ExportFormat

        client = MagicMock()
        mock_get_client.return_value = client

        export_mock = MagicMock()
        export_mock.content = base64.b64encode(b"# Databricks notebook source\nprint('hi')").decode()
        client.workspace.export.return_value = export_mock

        status_mock = MagicMock()
        status_mock.language = "PYTHON"
        client.workspace.get_status.return_value = status_mock

        mcp = _register()
        _call_tool(mcp, "read_notebook", {
            "notebook_path": "/test",
            "format": "JUPYTER",
        })

        # Verify the API was called with SOURCE, not JUPYTER
        client.workspace.export.assert_called_once_with(
            "/test",
            format=ExportFormat.SOURCE,
        )

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_jupyter_no_execution_outputs(self, mock_get_client):
        """Reconstructed notebook has empty outputs (not available from SOURCE)."""
        import base64

        client = MagicMock()
        mock_get_client.return_value = client

        export_mock = MagicMock()
        export_mock.content = base64.b64encode(_PYTHON_SOURCE.encode()).decode()
        client.workspace.export.return_value = export_mock

        status_mock = MagicMock()
        status_mock.language = "PYTHON"
        client.workspace.get_status.return_value = status_mock

        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/test",
            "format": "JUPYTER",
        })

        nb = json.loads(result["content"])
        for cell in nb["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == []
                assert cell["execution_count"] is None

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_jupyter_api_error(self, mock_get_client):
        """API failure returns error JSON."""
        client = MagicMock()
        mock_get_client.return_value = client
        client.workspace.export.side_effect = Exception("NOT_FOUND")

        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/nonexistent",
            "format": "JUPYTER",
        })

        assert "error" in result
        assert "NOT_FOUND" in result["error"]

    def test_invalid_format(self):
        """Invalid format returns an error."""
        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/test",
            "format": "CSV",
        })

        assert "error" in result
        assert "Invalid format" in result["error"]

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_source_format_still_works(self, mock_get_client):
        """SOURCE format is unchanged by this feature."""
        import base64

        client = MagicMock()
        mock_get_client.return_value = client

        source = "# Databricks notebook source\nprint('hello')"
        export_mock = MagicMock()
        export_mock.content = base64.b64encode(source.encode()).decode()
        client.workspace.export.return_value = export_mock

        mcp = _register()
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/test",
            "format": "SOURCE",
        })

        assert result["format"] == "SOURCE"
        assert result["content"] == source


# ------------------------------------------------------------------
# _source_to_ipynb unit tests (internal helper)
# ------------------------------------------------------------------


class TestSourceToIpynb:
    """Direct tests for the _source_to_ipynb conversion function."""

    def test_empty_source(self):
        from databricks_advanced_mcp.tools.workspace_ops import _source_to_ipynb

        result = json.loads(_source_to_ipynb("", "python"))
        assert result["cells"] == []
        assert result["nbformat"] == 4

    def test_single_cell_no_separator(self):
        from databricks_advanced_mcp.tools.workspace_ops import _source_to_ipynb

        source = "# Databricks notebook source\nprint('hello')"
        nb = json.loads(_source_to_ipynb(source, "python"))
        assert len(nb["cells"]) == 1
        assert nb["cells"][0]["cell_type"] == "code"
        assert "print('hello')" in "".join(nb["cells"][0]["source"])

    def test_markdown_magic_cell(self):
        from databricks_advanced_mcp.tools.workspace_ops import _source_to_ipynb

        source = "# Databricks notebook source\n# COMMAND ----------\n%md\n# Hello World\nSome text"
        nb = json.loads(_source_to_ipynb(source, "python"))
        md_cells = [c for c in nb["cells"] if c["cell_type"] == "markdown"]
        assert len(md_cells) == 1
        assert "Hello World" in "".join(md_cells[0]["source"])

    def test_sql_magic_in_python_notebook(self):
        from databricks_advanced_mcp.tools.workspace_ops import _source_to_ipynb

        source = "# Databricks notebook source\nprint('hi')\n# COMMAND ----------\n%sql\nSELECT 1"
        nb = json.loads(_source_to_ipynb(source, "python"))
        assert len(nb["cells"]) == 2
        assert nb["cells"][0]["cell_type"] == "code"
        assert nb["cells"][1]["cell_type"] == "code"
        # SQL magic cell should have language metadata
        assert nb["cells"][1]["metadata"].get("language") == "sql"

    def test_kernel_metadata_python(self):
        from databricks_advanced_mcp.tools.workspace_ops import _source_to_ipynb

        nb = json.loads(_source_to_ipynb("print(1)", "python"))
        assert nb["metadata"]["kernelspec"]["name"] == "python3"
        assert nb["metadata"]["language_info"]["name"] == "python"

    def test_kernel_metadata_sql(self):
        from databricks_advanced_mcp.tools.workspace_ops import _source_to_ipynb

        nb = json.loads(_source_to_ipynb("SELECT 1", "sql"))
        assert nb["metadata"]["kernelspec"]["language"] == "sql"
        assert nb["metadata"]["language_info"]["name"] == "sql"
