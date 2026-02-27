"""Shared test fixtures for Databricks Advanced MCP tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks_advanced_mcp.graph.cache import GraphCache
from databricks_advanced_mcp.graph.models import DependencyGraph, Edge, EdgeType, Node, NodeType


# ------------------------------------------------------------------
# Sample data
# ------------------------------------------------------------------

SAMPLE_NOTEBOOK_PYTHON = """# Databricks notebook source

# COMMAND ----------

# Read raw data
df = spark.table("main.bronze.raw_events")

# COMMAND ----------

# Transform
from pyspark.sql import functions as F

cleaned = df.filter(F.col("event_type").isNotNull())

# COMMAND ----------

# Write to silver
cleaned.write.mode("overwrite").saveAsTable("main.silver.clean_events")

# COMMAND ----------

# %sql
# SELECT count(*) FROM main.gold.aggregated_events
"""

SAMPLE_NOTEBOOK_SQL = """-- Databricks notebook source

-- COMMAND ----------

SELECT *
FROM main.bronze.raw_orders
WHERE order_date >= '2024-01-01'

-- COMMAND ----------

INSERT INTO main.silver.clean_orders
SELECT order_id, customer_id, amount
FROM main.bronze.raw_orders
WHERE amount > 0

-- COMMAND ----------

MERGE INTO main.gold.daily_totals AS target
USING (
    SELECT order_date, SUM(amount) AS total
    FROM main.silver.clean_orders
    GROUP BY order_date
) AS source
ON target.order_date = source.order_date
WHEN MATCHED THEN UPDATE SET total = source.total
WHEN NOT MATCHED THEN INSERT *
"""

SAMPLE_DLT_NOTEBOOK = """# Databricks notebook source

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(name="bronze_events")
def bronze_events():
    return spark.readStream.table("main.raw.events")

# COMMAND ----------

@dlt.table(name="silver_events")
def silver_events():
    return dlt.read("bronze_events").filter(F.col("valid") == True)

# COMMAND ----------

@dlt.view(name="gold_summary")
def gold_summary():
    return dlt.read_stream("silver_events").groupBy("category").count()
"""


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def mock_workspace_client():
    """Create a mock Databricks WorkspaceClient."""
    client = MagicMock()
    
    # Mock workspace.export
    export_result = MagicMock()
    export_result.content = None
    client.workspace.export.return_value = export_result
    
    # Mock jobs.list
    client.jobs.list.return_value = []
    
    # Mock pipelines.list_pipelines
    client.pipelines.list_pipelines.return_value = []
    
    # Mock tables.get
    client.tables.get.return_value = MagicMock()
    
    # Mock tables.list
    client.tables.list.return_value = []
    
    # Mock statement_execution.execute_statement
    client.statement_execution.execute_statement.return_value = MagicMock()
    
    return client


@pytest.fixture
def sample_graph() -> DependencyGraph:
    """Create a sample dependency graph for testing."""
    graph = DependencyGraph()

    # Tables
    raw_events = Node(NodeType.TABLE, "main.bronze.raw_events", "raw_events")
    clean_events = Node(NodeType.TABLE, "main.silver.clean_events", "clean_events")
    agg_events = Node(NodeType.TABLE, "main.gold.aggregated_events", "aggregated_events")

    # Notebooks
    etl_nb = Node(NodeType.NOTEBOOK, "/Workspace/ETL/process_events", "process_events")
    report_nb = Node(NodeType.NOTEBOOK, "/Workspace/Reports/daily_report", "daily_report")

    # Job
    etl_job = Node(NodeType.JOB, "123", "ETL Job", metadata={"job_id": "123"})

    # Pipeline
    dlt_pipeline = Node(NodeType.PIPELINE, "456", "Events Pipeline")

    for node in [raw_events, clean_events, agg_events, etl_nb, report_nb, etl_job, dlt_pipeline]:
        graph.add_node(node)

    # Edges: raw -> etl_nb -> clean -> report_nb -> agg
    graph.add_edge(Edge(raw_events.id, etl_nb.id, EdgeType.READS_FROM))
    graph.add_edge(Edge(etl_nb.id, clean_events.id, EdgeType.WRITES_TO))
    graph.add_edge(Edge(clean_events.id, report_nb.id, EdgeType.READS_FROM))
    graph.add_edge(Edge(report_nb.id, agg_events.id, EdgeType.WRITES_TO))

    # Job triggers notebook
    graph.add_edge(Edge(etl_job.id, etl_nb.id, EdgeType.TRIGGERS))

    # Pipeline contains notebook
    graph.add_edge(Edge(dlt_pipeline.id, etl_nb.id, EdgeType.CONTAINS))

    return graph


@pytest.fixture(autouse=True)
def reset_cache():
    """Reset the GraphCache singleton before each test."""
    GraphCache.reset()
    yield
    GraphCache.reset()
