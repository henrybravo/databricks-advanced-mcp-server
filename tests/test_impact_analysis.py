"""Unit tests for impact analysis."""

from databricks_advanced_mcp.graph.cache import GraphCache
from databricks_advanced_mcp.graph.models import DependencyGraph
from databricks_advanced_mcp.tools.impact_analysis import (
    analyze_column_drop,
    analyze_pipeline_failure,
    analyze_schema_change,
)


class TestColumnDropAnalysis:
    """Tests for column drop impact analysis."""

    def test_column_drop_finds_downstream(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_column_drop(
            "main.bronze.raw_events",
            "event_id",
            sample_graph,
            cache,
        )

        assert report.change_type == "column_drop"
        assert report.total_affected_count > 0
        assert any("event_id" in a.reason for a in report.affected_assets)

    def test_column_drop_unknown_table(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_column_drop(
            "nonexistent.table",
            "col",
            sample_graph,
            cache,
        )

        assert report.total_affected_count == 0
        assert len(report.remediation) > 0

    def test_column_drop_has_risk_score(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_column_drop(
            "main.bronze.raw_events",
            "event_id",
            sample_graph,
            cache,
        )

        assert report.risk_score >= 0


class TestSchemaChangeAnalysis:
    """Tests for schema change impact analysis."""

    def test_schema_change_removal(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        changes = [
            {"action": "remove", "column": "old_col"},
            {"action": "add", "column": "new_col"},
        ]

        report = analyze_schema_change(
            "main.bronze.raw_events",
            changes,
            sample_graph,
            cache,
        )

        assert report.change_type == "schema_change"
        assert report.total_affected_count > 0

    def test_schema_change_type_modification(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        changes = [{"action": "modify", "column": "amount", "new_type": "DOUBLE"}]

        report = analyze_schema_change(
            "main.bronze.raw_events",
            changes,
            sample_graph,
            cache,
        )

        assert report.change_type == "schema_change"
        assert any("Type-changed" in a.reason for a in report.affected_assets)


class TestPipelineFailureAnalysis:
    """Tests for pipeline failure impact analysis."""

    def test_pipeline_failure_finds_downstream(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_pipeline_failure("456", sample_graph, cache)

        assert report.change_type == "pipeline_failure"
        # Pipeline 456 contains etl_nb which writes to clean_events
        assert report.total_affected_count > 0

    def test_job_failure_analysis(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_pipeline_failure("123", sample_graph, cache)

        assert report.change_type == "pipeline_failure"
        assert report.total_affected_count > 0

    def test_unknown_pipeline(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_pipeline_failure("999", sample_graph, cache)

        assert report.total_affected_count == 0

    def test_report_serialization(self, sample_graph: DependencyGraph):
        cache = GraphCache.get_instance()
        cache.set(sample_graph)

        report = analyze_column_drop(
            "main.bronze.raw_events",
            "col",
            sample_graph,
            cache,
        )

        d = report.to_dict()
        assert "change_description" in d
        assert "affected_assets" in d
        assert "risk_score" in d
        assert "remediation" in d
