"""Unit tests for workspace listing tool and GraphBuilder workspace integration."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from databricks_advanced_mcp.graph.builder import GraphBuilder
from databricks_advanced_mcp.graph.models import (
    DependencyGraph,
    Edge,
    EdgeType,
    Node,
    NodeType,
)


# ------------------------------------------------------------------
# Helpers to create mock workspace objects
# ------------------------------------------------------------------


def _make_object_info(path: str, object_type: str, language: str | None = None):
    """Create a mock ObjectInfo with the given attributes."""
    from databricks.sdk.service.workspace import ObjectType

    obj = MagicMock()
    obj.path = path

    type_map = {
        "NOTEBOOK": ObjectType.NOTEBOOK,
        "DIRECTORY": ObjectType.DIRECTORY,
        "FILE": ObjectType.FILE,
    }
    obj.object_type = type_map.get(object_type, ObjectType.FILE)

    if language:
        lang_mock = MagicMock()
        lang_mock.value = language
        obj.language = lang_mock
    else:
        obj.language = None

    return obj


def _tool_names(mcp) -> list[str]:
    """Get registered tool names from a FastMCP instance."""
    tools = asyncio.run(mcp.list_tools())
    return [t.name for t in tools]


# ------------------------------------------------------------------
# Tests for list_workspace_notebooks tool
# ------------------------------------------------------------------


class TestListWorkspaceNotebooks:
    """Tests for the list_workspace_notebooks MCP tool."""

    def test_tool_registered(self):
        """Tool is registered with FastMCP."""
        from databricks_advanced_mcp.tools.workspace_listing import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        assert "list_workspace_notebooks" in _tool_names(mcp)

    @patch("databricks_advanced_mcp.tools.workspace_listing.get_workspace_client")
    def test_successful_listing(self, mock_get_client):
        """List notebooks at a specific path returns correct JSON."""
        client = MagicMock()
        mock_get_client.return_value = client

        nb1 = _make_object_info("/Workspace/Users/team/etl", "NOTEBOOK", "PYTHON")
        dir1 = _make_object_info("/Workspace/Users/team/subdir", "DIRECTORY")
        nb2 = _make_object_info("/Workspace/Users/team/subdir/report", "NOTEBOOK", "SQL")

        client.workspace.list.side_effect = [
            [nb1, dir1],  # root listing
            [nb2],  # subdir listing
        ]

        # Import the module and call the internal helper + tool logic directly
        from databricks_advanced_mcp.tools import workspace_listing

        result = json.loads(
            workspace_listing.register.__module__  # just to ensure import
            and workspace_listing._list_notebooks_iterative(client, "/Workspace/Users/team", max_depth=10)
            and "not used"  # won't reach here
        ) if False else None

        # Instead, test via the _list_notebooks_iterative helper
        notebooks = workspace_listing._list_notebooks_iterative(
            client, "/Workspace/Users/team", max_depth=10
        )
        assert len(notebooks) == 2
        paths = [nb["path"] for nb in notebooks]
        assert "/Workspace/Users/team/etl" in paths
        assert "/Workspace/Users/team/subdir/report" in paths

    @patch("databricks_advanced_mcp.tools.workspace_listing.get_workspace_client")
    def test_path_not_found(self, mock_get_client):
        """Non-existent path returns JSON error from the tool."""
        client = MagicMock()
        mock_get_client.return_value = client
        client.workspace.list.side_effect = Exception(
            "RESOURCE_DOES_NOT_EXIST: Path not found"
        )

        from databricks_advanced_mcp.tools.workspace_listing import _list_notebooks_iterative

        # The iterative helper silently skips errors — returns empty
        notebooks = _list_notebooks_iterative(client, "/nonexistent", max_depth=10)
        assert notebooks == []

        # Also verify the tool-level error handling via register
        from databricks_advanced_mcp.tools.workspace_listing import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        # Call the tool via FastMCP's call_tool (async)
        result_raw = asyncio.run(
            mcp.call_tool("list_workspace_notebooks", {"path": "/nonexistent"})
        )
        # Extract text from ToolResult
        if hasattr(result_raw, "content"):
            content_item = result_raw.content[0]
            result_text = getattr(content_item, "text", None) or str(content_item)
        elif isinstance(result_raw, list):
            item = result_raw[0]
            result_text = getattr(item, "text", None) or str(item)
        else:
            result_text = str(result_raw)
        result = json.loads(result_text)
        assert "error" in result

    @patch("databricks_advanced_mcp.tools.workspace_listing.get_workspace_client")
    def test_permission_error_on_subdirectory(self, mock_get_client):
        """Permission error on a subdirectory skips it, continues listing."""
        client = MagicMock()
        mock_get_client.return_value = client

        nb1 = _make_object_info("/ws/nb1", "NOTEBOOK", "PYTHON")
        dir1 = _make_object_info("/ws/restricted", "DIRECTORY")
        dir2 = _make_object_info("/ws/open", "DIRECTORY")
        nb2 = _make_object_info("/ws/open/nb2", "NOTEBOOK", "PYTHON")

        def side_effect(path):
            if path == "/ws":
                return [nb1, dir1, dir2]
            elif path == "/ws/restricted":
                raise PermissionError("Access denied")
            elif path == "/ws/open":
                return [nb2]
            return []

        client.workspace.list.side_effect = side_effect

        from databricks_advanced_mcp.tools.workspace_listing import _list_notebooks_iterative

        notebooks = _list_notebooks_iterative(client, "/ws", max_depth=10)
        assert len(notebooks) == 2
        paths = [nb["path"] for nb in notebooks]
        assert "/ws/nb1" in paths
        assert "/ws/open/nb2" in paths

    @patch("databricks_advanced_mcp.tools.workspace_listing.get_workspace_client")
    def test_empty_directory(self, mock_get_client):
        """Empty directory returns empty list."""
        client = MagicMock()
        mock_get_client.return_value = client
        client.workspace.list.return_value = []

        from databricks_advanced_mcp.tools.workspace_listing import _list_notebooks_iterative

        notebooks = _list_notebooks_iterative(client, "/empty", max_depth=10)
        assert notebooks == []

    @patch("databricks_advanced_mcp.tools.workspace_listing.get_workspace_client")
    def test_max_depth_limiting(self, mock_get_client):
        """max_depth limits recursion depth."""
        client = MagicMock()
        mock_get_client.return_value = client

        dir1 = _make_object_info("/root/d1", "DIRECTORY")
        nb1 = _make_object_info("/root/d1/nb1", "NOTEBOOK", "PYTHON")
        dir2 = _make_object_info("/root/d1/d2", "DIRECTORY")
        nb2 = _make_object_info("/root/d1/d2/nb2", "NOTEBOOK", "PYTHON")

        def side_effect(path):
            if path == "/root":
                return [dir1]
            elif path == "/root/d1":
                return [nb1, dir2]
            elif path == "/root/d1/d2":
                return [nb2]
            return []

        client.workspace.list.side_effect = side_effect

        from databricks_advanced_mcp.tools.workspace_listing import _list_notebooks_iterative

        # max_depth=1: root(0) -> d1(1, listed) -> d2(2, NOT listed since 1 >= max_depth)
        notebooks = _list_notebooks_iterative(client, "/root", max_depth=1)
        assert len(notebooks) == 1
        paths = [nb["path"] for nb in notebooks]
        assert "/root/d1/nb1" in paths
        assert "/root/d1/d2/nb2" not in paths


# ------------------------------------------------------------------
# Tests for GraphBuilder._scan_workspace_notebooks
# ------------------------------------------------------------------


class TestGraphBuilderWorkspaceScanning:
    """Tests for GraphBuilder workspace notebook discovery."""

    def test_discovers_new_notebooks(self, mock_workspace_client):
        """Workspace listing discovers notebooks not found via jobs/pipelines."""
        from databricks.sdk.service.workspace import ObjectType

        nb_obj = MagicMock()
        nb_obj.path = "/Workspace/standalone/analysis"
        nb_obj.object_type = ObjectType.NOTEBOOK
        nb_obj.language = None

        mock_workspace_client.workspace.list.return_value = [nb_obj]

        # Mock export to return empty notebook content
        export_result = MagicMock()
        export_result.content = ""
        mock_workspace_client.workspace.export.return_value = export_result

        builder = GraphBuilder(mock_workspace_client)
        builder._scan_workspace_notebooks(path_prefix="/Workspace/standalone")

        node_id = "notebook::/Workspace/standalone/analysis"
        assert builder.graph.get_node(node_id) is not None

    def test_skips_already_discovered_notebooks(self, mock_workspace_client):
        """Notebooks already in the graph are not re-scanned."""
        from databricks.sdk.service.workspace import ObjectType

        # Pre-populate graph with a notebook node
        builder = GraphBuilder(mock_workspace_client)
        existing_nb = Node(
            node_type=NodeType.NOTEBOOK,
            fqn="/Workspace/ETL/process",
            name="process",
        )
        builder._graph.add_node(existing_nb)

        # Workspace listing returns the same notebook
        nb_obj = MagicMock()
        nb_obj.path = "/Workspace/ETL/process"
        nb_obj.object_type = ObjectType.NOTEBOOK
        nb_obj.language = None

        mock_workspace_client.workspace.list.return_value = [nb_obj]

        builder._scan_workspace_notebooks(path_prefix="/Workspace/ETL")

        # workspace.export should NOT have been called (notebook was skipped)
        mock_workspace_client.workspace.export.assert_not_called()

    def test_handles_api_errors_gracefully(self, mock_workspace_client):
        """API errors during workspace listing don't crash the builder."""
        mock_workspace_client.workspace.list.side_effect = Exception("API Error")

        builder = GraphBuilder(mock_workspace_client)
        # Should not raise
        builder._scan_workspace_notebooks(path_prefix="/Workspace")

        # Graph should be empty (no notebooks discovered)
        summary = builder.graph.summary()
        assert summary.node_count == 0

    def test_build_includes_workspace_notebooks(self, mock_workspace_client):
        """build() with workspace scope includes workspace-listed notebooks."""
        from databricks.sdk.service.workspace import ObjectType

        # No jobs or pipelines
        mock_workspace_client.jobs.list.return_value = []
        mock_workspace_client.pipelines.list_pipelines.return_value = []

        # Workspace listing returns one notebook
        nb_obj = MagicMock()
        nb_obj.path = "/Workspace/Users/me/my_notebook"
        nb_obj.object_type = ObjectType.NOTEBOOK
        nb_obj.language = None

        mock_workspace_client.workspace.list.return_value = [nb_obj]

        # Mock export to return empty content
        export_result = MagicMock()
        export_result.content = ""
        mock_workspace_client.workspace.export.return_value = export_result

        builder = GraphBuilder(mock_workspace_client)
        graph = builder.build(path_prefix="/Workspace/Users/me")

        node_id = "notebook::/Workspace/Users/me/my_notebook"
        assert graph.get_node(node_id) is not None
        summary = graph.summary()
        assert summary.node_counts_by_type.get("notebook", 0) >= 1
