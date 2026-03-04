"""Integration-style tests for MCP tool endpoints."""

import asyncio


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
        assert "scan_jobs" in names
        assert "scan_dlt_pipelines" in names
        assert "scan_dlt_pipeline" in names
        # Graph tools moved to graph_ops
        assert "build_dependency_graph" not in names
        assert "refresh_graph" not in names


class TestGraphOpsTools:
    """Tests for graph operations tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.graph_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "build_dependency_graph" in names
        assert "get_table_dependencies" in names
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
        assert "trigger_job_run" in names


class TestCatalogOpsTools:
    """Tests for catalog operations tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.catalog_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "list_catalogs" in names
        assert "list_schemas" in names
        assert "describe_schema" in names
        assert "create_schema" in names
        assert "drop_schema" in names


class TestComputeOpsTools:
    """Tests for compute operations tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.compute_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "list_clusters" in names
        assert "get_cluster_status" in names
        assert "start_cluster" in names
        assert "stop_cluster" in names
        assert "restart_cluster" in names


class TestWarehouseOpsTools:
    """Tests for warehouse operations tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.warehouse_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "list_warehouses" in names
        assert "get_warehouse_status" in names
        assert "start_warehouse" in names
        assert "stop_warehouse" in names


class TestVolumeOpsTools:
    """Tests for volume operations tools."""

    def test_tools_registered(self):
        from databricks_advanced_mcp.tools.volume_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "list_volumes" in names
        assert "get_volume_info" in names
        assert "list_volume_files" in names
        assert "read_volume_file" in names


class TestWorkspaceOpsExtended:
    """Tests for extended workspace ops tools."""

    def test_new_tools_registered(self):
        from databricks_advanced_mcp.tools.workspace_ops import register
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        names = _tool_names(mcp)
        assert "read_notebook" in names
        assert "delete_workspace_item" in names
        assert "get_workspace_status" in names
        # Original tools still present
        assert "create_job" in names
        assert "create_notebook" in names
        assert "workspace_upload" in names


class TestAllToolsRegistration:
    """Test that register_all_tools registers all 38 tools."""

    def test_total_tool_count(self):
        from databricks_advanced_mcp.tools import register_all_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_all_tools(mcp)

        names = _tool_names(mcp)
        assert len(names) == 43, f"Expected 43 tools, got {len(names)}: {sorted(names)}"
