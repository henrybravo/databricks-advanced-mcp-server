"""Tests for spec-compliance-fixes changes.

Covers:
 - Task 1.4/1.5: Stale graph handling (get_or_stale, stale warnings)
 - Task 2.2:     Singular DLT pipeline scan
 - Task 3.4:     Scoped graph builds
 - Task 4.6:     Code snippet extraction
 - Task 5.4:     Table statistics via DESCRIBE DETAIL
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from unittest.mock import MagicMock, patch

import pytest
from mcp.types import TextContent

from databricks_advanced_mcp.graph.cache import GraphCache
from databricks_advanced_mcp.graph.models import (
    DependencyGraph,
    Edge,
    EdgeType,
    Node,
    NodeType,
)
from databricks_advanced_mcp.reviewers.performance import (
    ReviewFinding,
    check_performance,
    extract_code_snippet,
)
from databricks_advanced_mcp.reviewers.standards import check_standards
from databricks_advanced_mcp.reviewers.suggestions import check_suggestions


def _text(result: object) -> str:
    """Extract text from the first content item of a call_tool result."""
    content = getattr(result, "content", None)
    assert content is not None and len(content) > 0
    item = content[0]
    assert isinstance(item, TextContent)
    return item.text


# ==================================================================
# Task 1.4 — Tests for get_or_stale()
# ==================================================================

class TestGetOrStale:
    """Tests for GraphCache.get_or_stale()."""

    def test_returns_none_when_empty(self):
        """get_or_stale() returns None when no graph has been set."""
        cache = GraphCache.get_instance()
        assert cache.get_or_stale() is None

    def test_returns_graph_when_fresh(self):
        """get_or_stale() returns the graph when it's fresh."""
        cache = GraphCache.get_instance()
        graph = DependencyGraph()
        cache.set(graph)
        assert cache.get_or_stale() is graph

    def test_returns_graph_when_stale(self):
        """get_or_stale() returns the graph even when TTL has expired."""
        cache = GraphCache.get_instance()
        graph = DependencyGraph()
        cache.set(graph)
        # Force staleness by setting timestamp in the past
        cache._timestamp = time.time() - cache.ttl - 100
        assert cache.is_stale() is True
        assert cache.get_or_stale() is graph

    def test_get_or_none_returns_none_when_stale(self):
        """Contrasting behavior: get_or_none() returns None when stale."""
        cache = GraphCache.get_instance()
        graph = DependencyGraph()
        cache.set(graph)
        cache._timestamp = time.time() - cache.ttl - 100
        assert cache.get_or_none() is None

    def test_returns_none_after_invalidate(self):
        """get_or_stale() returns None after explicit invalidation."""
        cache = GraphCache.get_instance()
        cache.set(DependencyGraph())
        cache.invalidate()
        assert cache.get_or_stale() is None


# ==================================================================
# Task 1.5 — Stale-graph behavior in tools
# ==================================================================

class TestStaleGraphInTools:
    """Stale graph is served with a warning flag instead of an error."""

    def test_get_table_dependencies_stale_warning(self, sample_graph):
        """get_table_dependencies returns results with stale_warning=True."""
        cache = GraphCache.get_instance()
        cache.set(sample_graph)
        cache._timestamp = time.time() - cache.ttl - 100  # make stale

        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        # Call the tool directly
        tools = asyncio.run(mcp.list_tools())
        tool_fn = None
        for t in tools:
            if t.name == "get_table_dependencies":
                tool_fn = t
                break
        assert tool_fn is not None

        # The tool is registered as a closure; invoke via the module-level function
        # We need to call it through the registered tool mechanism
        # Easier: call the underlying function via the MCP framework
        result_json = asyncio.run(
            mcp.call_tool("get_table_dependencies", {"table_name": "main.bronze.raw_events"})
        )
        result = json.loads(_text(result_json))

        assert result["stale_warning"] is True
        assert result["graph_timestamp"] is not None
        # Should still return actual data, not an error
        assert "error" not in result

    def test_get_table_dependencies_no_graph_error(self):
        """get_table_dependencies returns error when no graph exists."""
        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("get_table_dependencies", {"table_name": "main.bronze.raw_events"})
        )
        result = json.loads(_text(result_json))
        assert "error" in result

    def test_analyze_impact_stale_warning(self, sample_graph):
        """analyze_impact returns results with stale_warning when graph is stale."""
        cache = GraphCache.get_instance()
        cache.set(sample_graph)
        cache._timestamp = time.time() - cache.ttl - 100

        from databricks_advanced_mcp.tools.impact_analysis import (
            analyze_column_drop,
        )

        report = analyze_column_drop(
            "main.bronze.raw_events",
            "event_id",
            sample_graph,
            cache,
        )

        assert report.stale_warning is True
        assert report.graph_timestamp > 0


# ==================================================================
# Task 2.2 — Singular DLT pipeline scan
# ==================================================================

class TestScanDltPipeline:
    """Tests for scan_dlt_pipeline tool."""

    def test_tool_registered(self):
        """scan_dlt_pipeline is registered as an MCP tool."""
        from databricks_advanced_mcp.tools.dependency_scanner import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = [t.name for t in asyncio.run(mcp.list_tools())]
        assert "scan_dlt_pipeline" in names

    @patch("databricks_advanced_mcp.tools.dependency_scanner.get_workspace_client")
    def test_successful_scan(self, mock_get_client):
        """scan_dlt_pipeline returns pipeline info for a valid ID."""
        from databricks_advanced_mcp.tools.dependency_scanner import register
        from fastmcp import FastMCP

        # Mock pipeline detail
        mock_client = MagicMock()
        mock_spec = MagicMock()
        mock_spec.name = "test_pipeline"
        mock_spec.target = "my_schema"
        mock_spec.catalog = "my_catalog"
        mock_spec.libraries = []
        mock_detail = MagicMock()
        mock_detail.spec = mock_spec
        mock_client.pipelines.get.return_value = mock_detail
        mock_get_client.return_value = mock_client

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("scan_dlt_pipeline", {"pipeline_id": "abc-123"})
        )
        result = json.loads(_text(result_json))

        assert result["pipeline_id"] == "abc-123"
        assert result["name"] == "test_pipeline"
        assert "error" not in result

    @patch("databricks_advanced_mcp.tools.dependency_scanner.get_workspace_client")
    def test_pipeline_not_found(self, mock_get_client):
        """scan_dlt_pipeline returns error for non-existent pipeline."""
        from databricks_advanced_mcp.tools.dependency_scanner import register
        from fastmcp import FastMCP

        mock_client = MagicMock()
        mock_client.pipelines.get.side_effect = Exception("Pipeline not found")
        mock_get_client.return_value = mock_client

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("scan_dlt_pipeline", {"pipeline_id": "nonexistent"})
        )
        result = json.loads(_text(result_json))
        assert "error" in result


# ==================================================================
# Task 3.4 — Scoped graph builds
# ==================================================================

class TestScopedGraphBuilds:
    """Tests for scope/path parameters on build_dependency_graph."""

    def test_workspace_scope_accepted(self):
        """scope='workspace' is valid (no error)."""
        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        with patch("databricks_advanced_mcp.tools.graph_ops.get_workspace_client") as m:
            mock_client = MagicMock()
            mock_client.jobs.list.return_value = []
            mock_client.pipelines.list_pipelines.return_value = []
            m.return_value = mock_client

            result_json = asyncio.run(
                mcp.call_tool("build_dependency_graph", {"scope": "workspace"})
            )
            result = json.loads(_text(result_json))
            assert "error" not in result
            assert result.get("scope") == "workspace"

    def test_path_scope_filters(self):
        """scope='path' with a valid path is accepted."""
        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        with patch("databricks_advanced_mcp.tools.graph_ops.get_workspace_client") as m:
            mock_client = MagicMock()
            mock_client.jobs.list.return_value = []
            mock_client.pipelines.list_pipelines.return_value = []
            m.return_value = mock_client

            result_json = asyncio.run(
                mcp.call_tool(
                    "build_dependency_graph",
                    {"scope": "path", "path": "/Workspace/team/project"},
                )
            )
            result = json.loads(_text(result_json))
            assert "error" not in result
            assert result.get("scope") == "path"
            assert result.get("path") == "/Workspace/team/project"

    def test_path_scope_missing_path_error(self):
        """scope='path' without a path value returns an error."""
        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("build_dependency_graph", {"scope": "path", "path": ""})
        )
        result = json.loads(_text(result_json))
        assert "error" in result

    def test_invalid_scope_error(self):
        """Invalid scope value returns an error."""
        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("build_dependency_graph", {"scope": "invalid"})
        )
        result = json.loads(_text(result_json))
        assert "error" in result

    def test_path_prefix_filters_notebook_tasks(self):
        """GraphBuilder with path_prefix skips notebooks outside the prefix."""
        from databricks_advanced_mcp.graph.builder import GraphBuilder

        mock_client = MagicMock()

        # Set up a job with two notebook tasks
        mock_task_in = MagicMock()
        mock_task_in.notebook_task = MagicMock()
        mock_task_in.notebook_task.notebook_path = "/Workspace/team/nb1"
        mock_task_in.sql_task = None
        mock_task_in.pipeline_task = None
        mock_task_in.task_key = "task_in"

        mock_task_out = MagicMock()
        mock_task_out.notebook_task = MagicMock()
        mock_task_out.notebook_task.notebook_path = "/Workspace/other/nb2"
        mock_task_out.sql_task = None
        mock_task_out.pipeline_task = None
        mock_task_out.task_key = "task_out"

        mock_job = MagicMock()
        mock_job.job_id = 1
        mock_job.settings.name = "Test Job"
        mock_job.settings.tasks = [mock_task_in, mock_task_out]

        mock_client.jobs.list.return_value = [mock_job]
        mock_client.jobs.get.return_value = mock_job
        mock_client.pipelines.list_pipelines.return_value = []

        # Mock notebook export to prevent actual API calls
        mock_export = MagicMock()
        mock_export.content = ""
        mock_client.workspace.export.return_value = mock_export

        builder = GraphBuilder(mock_client)
        graph = builder.build(path_prefix="/Workspace/team")

        # Only nb1 should be in the graph, not nb2
        node_ids = list(graph.graph.nodes())
        nb1_id = f"{NodeType.NOTEBOOK.value}::/Workspace/team/nb1"
        nb2_id = f"{NodeType.NOTEBOOK.value}::/Workspace/other/nb2"

        assert nb1_id in node_ids
        assert nb2_id not in node_ids


# ==================================================================
# Task 4.6 — Code snippet extraction
# ==================================================================

class TestCodeSnippetExtraction:
    """Tests for extract_code_snippet and snippet population in rules."""

    def test_normal_match(self):
        """extract_code_snippet returns context around a match."""
        content = "line0\nline1\ndf.collect()\nline3\nline4\nline5"
        match = re.search(r"\.collect\(\)", content)
        snippet = extract_code_snippet(content, match)
        assert "df.collect()" in snippet
        assert "line0" in snippet
        assert "line1" in snippet
        assert "line3" in snippet
        assert "line4" in snippet

    def test_match_near_start(self):
        """Snippet works when match is at the very first line."""
        content = "df.collect()\nline1\nline2\nline3"
        match = re.search(r"\.collect\(\)", content)
        snippet = extract_code_snippet(content, match)
        assert "df.collect()" in snippet
        # Should include lines after
        assert "line1" in snippet

    def test_match_near_end(self):
        """Snippet works when match is at the last line."""
        content = "line0\nline1\nline2\ndf.collect()"
        match = re.search(r"\.collect\(\)", content)
        snippet = extract_code_snippet(content, match)
        assert "df.collect()" in snippet
        assert "line1" in snippet or "line2" in snippet

    def test_empty_on_no_match(self):
        """Returns empty string when match is None."""
        snippet = extract_code_snippet("hello", None)
        assert snippet == ""

    def test_max_lines_cap(self):
        """Snippet is capped at max_lines."""
        content = "\n".join(f"line{i}" for i in range(20))
        match = re.search(r"line10", content)
        snippet = extract_code_snippet(content, match, context_lines=10, max_lines=5)
        assert len(snippet.splitlines()) <= 5

    def test_performance_finding_has_snippet(self):
        """check_performance populates code_snippet on findings."""
        code = "results = df.collect()"
        findings = check_performance(code, 0, "python")
        perf = [f for f in findings if f.rule_id == "PERF002"]
        assert len(perf) > 0
        assert perf[0].code_snippet != ""
        assert "collect()" in perf[0].code_snippet

    def test_standards_finding_has_snippet(self):
        """check_standards populates code_snippet on findings."""
        code = 'password = "secret123"'
        findings = check_standards(code, 0, "python")
        cred = [f for f in findings if f.rule_id == "STD020"]
        assert len(cred) > 0
        assert cred[0].code_snippet != ""

    def test_suggestions_finding_has_snippet(self):
        """check_suggestions populates code_snippet on findings."""
        code = "df.repartition(1).write.parquet('out')"
        findings = check_suggestions(code, 0, "python")
        opt = [f for f in findings if f.rule_id == "OPT012"]
        assert len(opt) > 0
        assert opt[0].code_snippet != ""
        assert "repartition(1)" in opt[0].code_snippet

    def test_sql_suggestion_snippet(self):
        """SQL optimization rules also populate code_snippet."""
        code = "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET *"
        findings = check_suggestions(code, 0, "sql")
        merge = [f for f in findings if f.rule_id == "OPT001"]
        assert len(merge) > 0
        assert merge[0].code_snippet != ""


# ==================================================================
# Task 5.4 — Table statistics via DESCRIBE DETAIL
# ==================================================================

class TestDescribeDetail:
    """Tests for _describe_detail helper and integration."""

    def test_delta_table_returns_stats(self):
        """DESCRIBE DETAIL returns numRecords and sizeInBytes for Delta."""
        from databricks_advanced_mcp.tools.table_info import _describe_detail

        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"
        # Simulate StatementState.SUCCEEDED comparison
        from databricks.sdk.service.sql import StatementState
        mock_response.status.state = StatementState.SUCCEEDED

        # Build manifest with columns
        col_names = ["format", "id", "name", "numRecords", "sizeInBytes", "location"]
        mock_cols = []
        for name in col_names:
            mock_col = MagicMock()
            mock_col.name = name
            mock_cols.append(mock_col)
        mock_response.manifest.schema.columns = mock_cols
        mock_response.result.data_array = [
            ["delta", "abc-123", "my_table", "1000", "524288", "/some/path"]
        ]

        with patch("databricks_advanced_mcp.tools.table_info.get_workspace_client") as mock_get:
            mock_client = MagicMock()
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get.return_value = mock_client

            row_count, size_bytes = _describe_detail(
                "catalog.schema.table", "wh-123", "catalog", "schema"
            )

        assert row_count == 1000
        assert size_bytes == 524288

    def test_view_returns_null(self):
        """Views fail gracefully — both fields are None."""
        from databricks_advanced_mcp.tools.table_info import _describe_detail

        mock_response = MagicMock()
        from databricks.sdk.service.sql import StatementState
        mock_response.status.state = StatementState.FAILED
        mock_response.status.error = MagicMock()
        mock_response.status.error.message = "DESCRIBE DETAIL not supported for views"

        with patch("databricks_advanced_mcp.tools.table_info.get_workspace_client") as mock_get:
            mock_client = MagicMock()
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get.return_value = mock_client

            row_count, size_bytes = _describe_detail(
                "catalog.schema.my_view", "wh-123", "catalog", "schema"
            )

        assert row_count is None
        assert size_bytes is None

    def test_query_failure_returns_null_gracefully(self):
        """Exception during DESCRIBE DETAIL returns (None, None)."""
        from databricks_advanced_mcp.tools.table_info import _describe_detail

        with patch("databricks_advanced_mcp.tools.table_info.get_workspace_client") as mock_get:
            mock_client = MagicMock()
            mock_client.statement_execution.execute_statement.side_effect = Exception(
                "Warehouse not running"
            )
            mock_get.return_value = mock_client

            row_count, size_bytes = _describe_detail(
                "catalog.schema.table", "wh-123", "catalog", "schema"
            )

        assert row_count is None
        assert size_bytes is None

    @patch("databricks_advanced_mcp.tools.table_info._describe_detail")
    @patch("databricks_advanced_mcp.tools.table_info.get_workspace_client")
    @patch("databricks_advanced_mcp.tools.table_info.get_settings")
    def test_get_table_info_includes_stats(self, mock_settings, mock_get_client, mock_describe):
        """get_table_info populates row_count and size_bytes for managed tables."""
        from databricks_advanced_mcp.tools.table_info import register
        from fastmcp import FastMCP

        # Mock settings
        settings = MagicMock()
        settings.databricks_catalog = "cat"
        settings.databricks_schema = "sch"
        settings.databricks_warehouse_id = "wh-1"
        mock_settings.return_value = settings

        # Mock tables.get
        mock_table = MagicMock()
        mock_table.full_name = "cat.sch.my_table"
        mock_table.table_type = "MANAGED"
        mock_table.columns = []
        mock_table.properties = {}
        mock_table.storage_location = "/some/path"
        mock_table.created_at = None
        mock_table.updated_at = None
        mock_table.comment = None
        mock_client = MagicMock()
        mock_client.tables.get.return_value = mock_table
        mock_get_client.return_value = mock_client

        # Mock DESCRIBE DETAIL returns stats
        mock_describe.return_value = (5000, 1048576)

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("get_table_info", {"table_name": "my_table"})
        )
        result = json.loads(_text(result_json))

        assert result["row_count"] == 5000
        assert result["size_bytes"] == 1048576

    @patch("databricks_advanced_mcp.tools.table_info._describe_detail")
    @patch("databricks_advanced_mcp.tools.table_info.get_workspace_client")
    @patch("databricks_advanced_mcp.tools.table_info.get_settings")
    def test_get_table_info_skips_views(self, mock_settings, mock_get_client, mock_describe):
        """get_table_info skips DESCRIBE DETAIL for views."""
        from databricks_advanced_mcp.tools.table_info import register
        from fastmcp import FastMCP

        settings = MagicMock()
        settings.databricks_catalog = "cat"
        settings.databricks_schema = "sch"
        settings.databricks_warehouse_id = "wh-1"
        mock_settings.return_value = settings

        mock_table = MagicMock()
        mock_table.full_name = "cat.sch.my_view"
        mock_table.table_type = "VIEW"
        mock_table.columns = []
        mock_table.properties = {}
        mock_table.storage_location = None
        mock_table.created_at = None
        mock_table.updated_at = None
        mock_table.comment = None
        mock_client = MagicMock()
        mock_client.tables.get.return_value = mock_table
        mock_get_client.return_value = mock_client

        mcp = FastMCP("test")
        register(mcp)

        result_json = asyncio.run(
            mcp.call_tool("get_table_info", {"table_name": "my_view"})
        )
        result = json.loads(_text(result_json))

        # _describe_detail should not have been called for a VIEW
        mock_describe.assert_not_called()
        assert result["row_count"] is None
        assert result["size_bytes"] is None
