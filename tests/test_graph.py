"""Unit tests for graph models and builder."""

from databricks_advanced_mcp.graph.models import (
    DependencyGraph,
    Edge,
    EdgeType,
    GraphSummary,
    Node,
    NodeType,
)


class TestNode:
    """Tests for Node dataclass."""

    def test_node_id(self):
        node = Node(NodeType.TABLE, "main.schema.table", "table")
        assert node.id == "table::main.schema.table"

    def test_node_id_job(self):
        node = Node(NodeType.JOB, "123", "My Job")
        assert node.id == "job::123"


class TestDependencyGraph:
    """Tests for the DependencyGraph wrapper."""

    def test_add_and_get_node(self):
        graph = DependencyGraph()
        node = Node(NodeType.TABLE, "main.db.t1", "t1")
        graph.add_node(node)

        data = graph.get_node(node.id)
        assert data is not None
        assert data["fqn"] == "main.db.t1"

    def test_get_nonexistent_node(self):
        graph = DependencyGraph()
        assert graph.get_node("nonexistent::id") is None

    def test_add_edge(self):
        graph = DependencyGraph()
        n1 = Node(NodeType.TABLE, "t1", "t1")
        n2 = Node(NodeType.NOTEBOOK, "/nb", "nb")
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_edge(Edge(n1.id, n2.id, EdgeType.READS_FROM))

        assert graph.graph.has_edge(n1.id, n2.id)

    def test_get_downstream(self, sample_graph):
        # raw_events -> etl_nb -> clean_events -> report_nb -> agg_events
        downstream = sample_graph.get_downstream("table::main.bronze.raw_events")
        assert "notebook::/Workspace/ETL/process_events" in downstream
        assert "table::main.silver.clean_events" in downstream
        assert "table::main.gold.aggregated_events" in downstream

    def test_get_upstream(self, sample_graph):
        upstream = sample_graph.get_upstream("table::main.gold.aggregated_events")
        assert "notebook::/Workspace/Reports/daily_report" in upstream
        assert "table::main.silver.clean_events" in upstream

    def test_get_downstream_with_depth(self, sample_graph):
        downstream = sample_graph.get_downstream("table::main.bronze.raw_events", depth=1)
        assert "notebook::/Workspace/ETL/process_events" in downstream
        # Depth 1 should not reach clean_events (2 hops)
        assert "table::main.silver.clean_events" not in downstream

    def test_get_path(self, sample_graph):
        path = sample_graph.get_path(
            "table::main.bronze.raw_events",
            "table::main.gold.aggregated_events",
        )
        assert len(path) >= 3
        assert path[0] == "table::main.bronze.raw_events"
        assert path[-1] == "table::main.gold.aggregated_events"

    def test_get_path_no_path(self, sample_graph):
        path = sample_graph.get_path(
            "table::main.gold.aggregated_events",
            "table::main.bronze.raw_events",
        )
        assert path == []

    def test_summary(self, sample_graph):
        summary = sample_graph.summary()
        assert isinstance(summary, GraphSummary)
        assert summary.node_count == 7
        assert summary.edge_count == 6
        assert "table" in summary.node_counts_by_type
        assert "notebook" in summary.node_counts_by_type

    def test_clear(self):
        graph = DependencyGraph()
        graph.add_node(Node(NodeType.TABLE, "t1", "t1"))
        assert graph.graph.number_of_nodes() == 1
        graph.clear()
        assert graph.graph.number_of_nodes() == 0

    def test_to_dict(self, sample_graph):
        d = sample_graph.to_dict()
        assert "nodes" in d
        assert "edges" in d
        assert len(d["nodes"]) == 7
        assert len(d["edges"]) == 6
