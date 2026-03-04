"""Unit tests for new tool modules: catalog_ops, compute_ops, warehouse_ops,
volume_ops, graph_ops, and extended workspace_ops tools.
"""

from __future__ import annotations

import asyncio
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

from fastmcp import FastMCP


def _tool_names(mcp: FastMCP) -> list[str]:
    tools = asyncio.run(mcp.list_tools())
    return [t.name for t in tools]


def _call_tool(mcp: FastMCP, name: str, args: dict) -> dict:
    result = asyncio.run(mcp.call_tool(name, args))
    text = result.content[0].text
    return json.loads(text)


# ==================================================================
# catalog_ops
# ==================================================================


class TestListCatalogs:

    @patch("databricks_advanced_mcp.tools.catalog_ops.get_workspace_client")
    def test_list_catalogs_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.catalog_ops import register

        cat = MagicMock()
        cat.name = "main"
        cat.owner = "admin"
        cat.comment = "Default catalog"
        cat.catalog_type = None
        cat.isolation_mode = None
        cat.created_at = None

        client = MagicMock()
        client.catalogs.list.return_value = [cat]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_catalogs", {})

        assert result["catalog_count"] == 1
        assert result["catalogs"][0]["name"] == "main"

    @patch("databricks_advanced_mcp.tools.catalog_ops.get_workspace_client")
    def test_list_catalogs_error(self, mock_get_client):
        from databricks_advanced_mcp.tools.catalog_ops import register

        client = MagicMock()
        client.catalogs.list.side_effect = Exception("forbidden")
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_catalogs", {})
        assert "error" in result


class TestListSchemas:

    @patch("databricks_advanced_mcp.tools.catalog_ops.get_workspace_client")
    def test_list_schemas_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.catalog_ops import register

        schema = MagicMock()
        schema.name = "default"
        schema.full_name = "main.default"
        schema.owner = "admin"
        schema.comment = ""
        schema.created_at = None

        client = MagicMock()
        client.schemas.list.return_value = [schema]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_schemas", {"catalog": "main"})

        assert result["schema_count"] == 1
        assert result["catalog"] == "main"


class TestDescribeSchema:

    @patch("databricks_advanced_mcp.tools.catalog_ops.get_workspace_client")
    def test_describe_schema_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.catalog_ops import register

        info = MagicMock()
        info.name = "default"
        info.full_name = "main.default"
        info.catalog_name = "main"
        info.owner = "admin"
        info.comment = "Default schema"
        info.properties = {"key": "val"}
        info.created_at = None
        info.updated_at = None

        client = MagicMock()
        client.schemas.get.return_value = info
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "describe_schema", {"catalog": "main", "schema": "default"})

        assert result["name"] == "default"
        assert result["comment"] == "Default schema"


class TestCreateSchema:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.catalog_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "create_schema", {
            "catalog": "main", "schema": "staging"
        })
        assert result["action"] == "preview"
        assert "Would create" in result["message"]

    @patch("databricks_advanced_mcp.tools.catalog_ops.get_workspace_client")
    def test_create_confirmed(self, mock_get_client):
        from databricks_advanced_mcp.tools.catalog_ops import register

        info = MagicMock()
        info.full_name = "main.staging"
        info.owner = "admin"
        info.comment = ""

        client = MagicMock()
        client.schemas.create.return_value = info
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "create_schema", {
            "catalog": "main", "schema": "staging", "confirm": True
        })
        assert result["action"] == "created"


class TestDropSchema:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.catalog_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "drop_schema", {
            "catalog": "main", "schema": "old"
        })
        assert result["action"] == "preview"
        assert "DESTRUCTIVE" in result["warning"]

    @patch("databricks_advanced_mcp.tools.catalog_ops.get_workspace_client")
    def test_drop_confirmed(self, mock_get_client):
        from databricks_advanced_mcp.tools.catalog_ops import register

        client = MagicMock()
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "drop_schema", {
            "catalog": "main", "schema": "old", "confirm": True
        })
        assert result["action"] == "dropped"
        client.schemas.delete.assert_called_once_with("main.old")


# ==================================================================
# compute_ops
# ==================================================================


class TestListClusters:

    @patch("databricks_advanced_mcp.tools.compute_ops.get_workspace_client")
    def test_list_clusters_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.compute_ops import register

        cluster = MagicMock()
        cluster.cluster_id = "abc-123"
        cluster.cluster_name = "dev-cluster"
        cluster.state = "RUNNING"
        cluster.creator_user_name = "user@example.com"
        cluster.spark_version = "14.3.x-scala2.12"
        cluster.node_type_id = "Standard_DS3_v2"
        cluster.driver_node_type_id = "Standard_DS3_v2"
        cluster.autotermination_minutes = 120
        cluster.num_workers = 2

        client = MagicMock()
        client.clusters.list.return_value = [cluster]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_clusters", {})

        assert result["cluster_count"] == 1
        assert result["clusters"][0]["cluster_id"] == "abc-123"

    @patch("databricks_advanced_mcp.tools.compute_ops.get_workspace_client")
    def test_list_clusters_with_filter(self, mock_get_client):
        from databricks_advanced_mcp.tools.compute_ops import register

        c1 = MagicMock()
        c1.cluster_name = "dev-cluster"
        c1.cluster_id = "1"
        c1.state = "RUNNING"
        c1.creator_user_name = ""
        c1.spark_version = ""
        c1.node_type_id = ""
        c1.driver_node_type_id = ""
        c1.autotermination_minutes = 0
        c1.num_workers = 0

        c2 = MagicMock()
        c2.cluster_name = "prod-cluster"
        c2.cluster_id = "2"
        c2.state = "RUNNING"
        c2.creator_user_name = ""
        c2.spark_version = ""
        c2.node_type_id = ""
        c2.driver_node_type_id = ""
        c2.autotermination_minutes = 0
        c2.num_workers = 0

        client = MagicMock()
        client.clusters.list.return_value = [c1, c2]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_clusters", {"name_filter": "prod"})

        assert result["cluster_count"] == 1
        assert result["clusters"][0]["cluster_name"] == "prod-cluster"


class TestStartCluster:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.compute_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "start_cluster", {"cluster_id": "abc-123"})
        assert result["action"] == "preview"

    @patch("databricks_advanced_mcp.tools.compute_ops.get_workspace_client")
    def test_start_confirmed(self, mock_get_client):
        from databricks_advanced_mcp.tools.compute_ops import register

        client = MagicMock()
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "start_cluster", {"cluster_id": "abc-123", "confirm": True})
        assert result["action"] == "started"
        client.clusters.start.assert_called_once()


class TestStopCluster:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.compute_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "stop_cluster", {"cluster_id": "abc-123"})
        assert result["action"] == "preview"

    @patch("databricks_advanced_mcp.tools.compute_ops.get_workspace_client")
    def test_stop_confirmed(self, mock_get_client):
        from databricks_advanced_mcp.tools.compute_ops import register

        client = MagicMock()
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "stop_cluster", {"cluster_id": "abc-123", "confirm": True})
        assert result["action"] == "stopped"


class TestRestartCluster:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.compute_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "restart_cluster", {"cluster_id": "abc-123"})
        assert result["action"] == "preview"


# ==================================================================
# warehouse_ops
# ==================================================================


class TestListWarehouses:

    @patch("databricks_advanced_mcp.tools.warehouse_ops.get_workspace_client")
    def test_list_warehouses_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.warehouse_ops import register

        w = MagicMock()
        w.id = "wh-001"
        w.name = "Starter Warehouse"
        w.state = "RUNNING"
        w.cluster_size = "2X-Small"
        w.warehouse_type = "PRO"
        w.creator_name = "admin"
        w.num_clusters = 1
        w.auto_stop_mins = 15
        w.enable_serverless_compute = False

        client = MagicMock()
        client.warehouses.list.return_value = [w]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_warehouses", {})

        assert result["warehouse_count"] == 1
        assert result["warehouses"][0]["id"] == "wh-001"


class TestStartWarehouse:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.warehouse_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "start_warehouse", {"warehouse_id": "wh-001"})
        assert result["action"] == "preview"

    @patch("databricks_advanced_mcp.tools.warehouse_ops.get_workspace_client")
    def test_start_confirmed(self, mock_get_client):
        from databricks_advanced_mcp.tools.warehouse_ops import register

        client = MagicMock()
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "start_warehouse", {"warehouse_id": "wh-001", "confirm": True})
        assert result["action"] == "started"


class TestStopWarehouse:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.warehouse_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "stop_warehouse", {"warehouse_id": "wh-001"})
        assert result["action"] == "preview"


# ==================================================================
# volume_ops
# ==================================================================


class TestListVolumes:

    @patch("databricks_advanced_mcp.tools.volume_ops.get_workspace_client")
    def test_list_volumes_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.volume_ops import register

        vol = MagicMock()
        vol.name = "raw_data"
        vol.full_name = "main.default.raw_data"
        vol.volume_type = "MANAGED"
        vol.catalog_name = "main"
        vol.schema_name = "default"
        vol.storage_location = "s3://bucket/path"
        vol.owner = "admin"
        vol.comment = ""
        vol.created_at = None

        client = MagicMock()
        client.volumes.list.return_value = [vol]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_volumes", {"catalog": "main", "schema": "default"})

        assert result["volume_count"] == 1
        assert result["volumes"][0]["name"] == "raw_data"


class TestGetVolumeInfo:

    @patch("databricks_advanced_mcp.tools.volume_ops.get_workspace_client")
    def test_get_volume_info_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.volume_ops import register

        vol = MagicMock()
        vol.name = "raw_data"
        vol.full_name = "main.default.raw_data"
        vol.volume_type = "MANAGED"
        vol.catalog_name = "main"
        vol.schema_name = "default"
        vol.storage_location = "s3://bucket/path"
        vol.owner = "admin"
        vol.comment = "Raw data volume"
        vol.created_at = None
        vol.updated_at = None

        client = MagicMock()
        client.volumes.read.return_value = vol
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "get_volume_info", {
            "volume_name": "raw_data", "catalog": "main", "schema": "default"
        })

        assert result["name"] == "raw_data"
        assert result["comment"] == "Raw data volume"


class TestListVolumeFiles:

    @patch("databricks_advanced_mcp.tools.volume_ops.get_workspace_client")
    def test_list_volume_files_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.volume_ops import register

        item = MagicMock()
        item.name = "data.csv"
        item.path = "/Volumes/main/default/raw/data.csv"
        item.is_directory = False
        item.file_size = 1024
        item.last_modified = None

        client = MagicMock()
        client.files.list_directory_contents.return_value = [item]
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "list_volume_files", {
            "volume_path": "/Volumes/main/default/raw"
        })

        assert result["item_count"] == 1
        assert result["items"][0]["name"] == "data.csv"


class TestReadVolumeFile:

    @patch("databricks_advanced_mcp.tools.volume_ops.get_workspace_client")
    def test_read_text_file(self, mock_get_client):
        from databricks_advanced_mcp.tools.volume_ops import register

        response = MagicMock()
        response.contents = BytesIO(b"hello,world\n1,2\n")
        client = MagicMock()
        client.files.download.return_value = response
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "read_volume_file", {
            "volume_path": "/Volumes/main/default/raw/data.csv"
        })

        assert result["encoding"] == "utf-8"
        assert "hello,world" in result["content"]

    @patch("databricks_advanced_mcp.tools.volume_ops.get_workspace_client")
    def test_read_binary_file(self, mock_get_client):
        from databricks_advanced_mcp.tools.volume_ops import register

        response = MagicMock()
        response.contents = BytesIO(b"\x89PNG\r\n\x1a\n\x00")
        client = MagicMock()
        client.files.download.return_value = response
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "read_volume_file", {
            "volume_path": "/Volumes/main/default/raw/image.png"
        })

        assert result["encoding"] == "base64"


# ==================================================================
# Extended workspace_ops (read_notebook, delete_workspace_item, get_workspace_status)
# ==================================================================


class TestReadNotebook:

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_read_notebook_success(self, mock_get_client):
        import base64
        from databricks_advanced_mcp.tools.workspace_ops import register

        export = MagicMock()
        export.content = base64.b64encode(b"# Databricks notebook source\nprint('hi')").decode()
        client = MagicMock()
        client.workspace.export.return_value = export
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/Workspace/Users/me/nb"
        })

        assert "Databricks notebook source" in result["content"]
        assert result["format"] == "SOURCE"

    def test_invalid_format(self):
        from databricks_advanced_mcp.tools.workspace_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "read_notebook", {
            "notebook_path": "/Workspace/Users/me/nb",
            "format": "INVALID",
        })
        assert "error" in result


class TestDeleteWorkspaceItem:

    def test_preview_mode(self):
        from databricks_advanced_mcp.tools.workspace_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "delete_workspace_item", {
            "path": "/Workspace/Users/me/old_nb"
        })
        assert result["action"] == "preview"
        assert "DESTRUCTIVE" in result["warning"]

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_delete_confirmed(self, mock_get_client):
        from databricks_advanced_mcp.tools.workspace_ops import register

        client = MagicMock()
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "delete_workspace_item", {
            "path": "/Workspace/Users/me/old_nb",
            "confirm": True,
        })
        assert result["action"] == "deleted"
        client.workspace.delete.assert_called_once()


class TestGetWorkspaceStatus:

    @patch("databricks_advanced_mcp.tools.workspace_ops.get_workspace_client")
    def test_get_status_success(self, mock_get_client):
        from databricks_advanced_mcp.tools.workspace_ops import register

        status = MagicMock()
        status.path = "/Workspace/Users/me/nb"
        status.object_type = "NOTEBOOK"
        status.object_id = 12345
        status.language = "PYTHON"
        status.created_at = None
        status.modified_at = None
        status.size = None

        client = MagicMock()
        client.workspace.get_status.return_value = status
        mock_get_client.return_value = client

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "get_workspace_status", {
            "path": "/Workspace/Users/me/nb"
        })

        assert result["object_type"] == "NOTEBOOK"
        assert result["object_id"] == 12345


# ==================================================================
# graph_ops
# ==================================================================


class TestGraphOpsBuildGraph:

    def test_invalid_scope(self):
        from databricks_advanced_mcp.tools.graph_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "build_dependency_graph", {"scope": "invalid"})
        assert "error" in result

    def test_path_scope_without_path(self):
        from databricks_advanced_mcp.tools.graph_ops import register

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "build_dependency_graph", {"scope": "path"})
        assert "error" in result


class TestGraphOpsGetTableDependencies:

    def test_no_graph_built(self):
        from databricks_advanced_mcp.graph.cache import GraphCache
        from databricks_advanced_mcp.tools.graph_ops import register

        # Reset the cache
        cache = GraphCache.get_instance()
        cache.invalidate()
        cache._graph = None

        mcp = FastMCP("test")
        register(mcp)
        result = _call_tool(mcp, "get_table_dependencies", {"table_name": "main.default.orders"})
        assert "error" in result
        assert "not built yet" in result["error"]
