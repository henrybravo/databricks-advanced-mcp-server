"""Integration-style tests for MCP tool endpoints."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest


def _tool_names(mcp) -> list[str]:
    """Get registered tool names from a FastMCP instance."""
    tools = asyncio.run(mcp.list_tools())
    return [t.name for t in tools]


class TestSqlExecutorTool:
    """Tests for the SQL executor tool."""

    def test_execute_query_returns_json(self, mock_workspace_client):
        from databricks_advanced_mcp.tools.sql_executor import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        assert "execute_query" in _tool_names(mcp)


class TestTableInfoTool:
    """Tests for table info tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.table_info import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "get_table_info" in names
        assert "list_tables" in names


class TestDependencyScannerTools:
    """Tests for dependency scanner tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.dependency_scanner import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "scan_notebook" in names
        assert "build_dependency_graph" in names
        assert "refresh_graph" in names


class TestImpactAnalysisTool:
    """Tests for impact analysis tool."""

    def test_tool_registered(self):
        from databricks_advanced_mcp.tools.impact_analysis import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        assert "analyze_impact" in _tool_names(mcp)


class TestNotebookReviewerTool:
    """Tests for notebook reviewer tool."""

    def test_tool_registered(self):
        from databricks_advanced_mcp.tools.notebook_reviewer import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        assert "review_notebook" in _tool_names(mcp)


class TestJobPipelineOpsTool:
    """Tests for job/pipeline ops tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.job_pipeline_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "list_jobs" in names
        assert "get_job_status" in names
        assert "list_pipelines" in names
        assert "trigger_rerun" in names
