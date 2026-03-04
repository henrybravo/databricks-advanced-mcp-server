"""Graph builder that orchestrates API scans and builds the dependency DAG.

Coordinates the parsers and Databricks APIs to construct a complete
DependencyGraph of workspace assets.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from databricks_advanced_mcp.graph.models import (
    DependencyGraph,
    Edge,
    EdgeType,
    Node,
    NodeType,
)
from databricks_advanced_mcp.parsers.dlt_parser import (
    extract_dlt_references_from_code,
    parse_dlt_pipeline_config,
)
from databricks_advanced_mcp.parsers.notebook_parser import parse_notebook
from databricks_advanced_mcp.parsers.sql_parser import TableReference

logger = logging.getLogger(__name__)


class GraphBuilder:
    """Builds a DependencyGraph by scanning workspace assets."""

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._graph = DependencyGraph()

    @property
    def graph(self) -> DependencyGraph:
        return self._graph

    def build(self, path_prefix: str = "") -> DependencyGraph:
        """Run all scans and build the complete dependency graph.

        Args:
            path_prefix: If non-empty, only scan notebooks whose path starts
                         with this prefix.  Jobs are still scanned, but notebook
                         tasks outside the prefix are skipped.

        Returns:
            The constructed DependencyGraph.
        """
        self._graph.clear()
        self._scan_jobs(path_prefix=path_prefix)
        self._scan_pipelines(path_prefix=path_prefix)
        self._scan_workspace_notebooks(path_prefix=path_prefix)
        return self._graph

    # ------------------------------------------------------------------
    # Job scanning
    # ------------------------------------------------------------------

    def _scan_jobs(self, path_prefix: str = "") -> None:
        """Scan all jobs and extract dependencies from their tasks."""
        try:
            jobs = list(self._client.jobs.list())
        except Exception as e:
            logger.warning("Failed to list jobs: %s", e)
            return

        for job in jobs:
            job_id = str(job.job_id)

            # Fetch full job details — list() may omit task definitions
            settings = job.settings
            try:
                job_detail = self._client.jobs.get(int(job_id))
                if job_detail.settings:
                    settings = job_detail.settings
            except Exception:
                pass  # Fall back to list() data

            job_name = settings.name if settings else f"job_{job_id}"

            job_node = Node(
                node_type=NodeType.JOB,
                fqn=job_id,
                name=job_name or f"job_{job_id}",
                metadata={"job_id": job_id},
            )
            self._graph.add_node(job_node)

            raw_tasks = (settings.tasks if settings else None) or []
            if not raw_tasks:
                continue

            for task in raw_tasks:
                self._process_job_task(job_node, task, path_prefix=path_prefix)

    def _process_job_task(self, job_node: Node, task: Any, path_prefix: str = "") -> None:
        """Process a single job task and extract dependencies."""
        # Notebook task
        if task.notebook_task:
            notebook_path = task.notebook_task.notebook_path

            # Skip notebooks outside the path prefix when scoped
            if path_prefix and not notebook_path.startswith(path_prefix):
                return

            refs = self._scan_notebook_path(notebook_path)

            nb_node = Node(
                node_type=NodeType.NOTEBOOK,
                fqn=notebook_path,
                name=notebook_path.split("/")[-1],
            )
            self._graph.add_node(nb_node)

            # Job triggers notebook
            self._graph.add_edge(Edge(
                source_id=job_node.id,
                target_id=nb_node.id,
                edge_type=EdgeType.TRIGGERS,
            ))

            self._add_table_edges(nb_node, refs)

        # SQL task
        if task.sql_task and task.sql_task.query:
            query_id = task.sql_task.query.query_id or "unknown"
            query_node = Node(
                node_type=NodeType.QUERY,
                fqn=f"query::{query_id}",
                name=f"sql_query_{query_id}",
            )
            self._graph.add_node(query_node)
            self._graph.add_edge(Edge(
                source_id=job_node.id,
                target_id=query_node.id,
                edge_type=EdgeType.TRIGGERS,
            ))

        # DLT pipeline task
        if task.pipeline_task:
            pipeline_id = task.pipeline_task.pipeline_id
            if pipeline_id:
                pipeline_node_id = f"{NodeType.PIPELINE.value}::{pipeline_id}"
                # Ensure pipeline node exists (may be created by _scan_pipelines)
                if not self._graph.get_node(pipeline_node_id):
                    self._graph.add_node(Node(
                        node_type=NodeType.PIPELINE,
                        fqn=pipeline_id,
                        name=f"pipeline_{pipeline_id}",
                    ))
                self._graph.add_edge(Edge(
                    source_id=job_node.id,
                    target_id=pipeline_node_id,
                    edge_type=EdgeType.TRIGGERS,
                ))

    # ------------------------------------------------------------------
    # Pipeline scanning
    # ------------------------------------------------------------------

    def _scan_pipelines(self, path_prefix: str = "") -> None:
        """Scan all DLT pipelines and extract dependencies."""
        try:
            pipelines = list(self._client.pipelines.list_pipelines())
        except Exception as e:
            logger.warning("Failed to list pipelines: %s", e)
            return

        for pipeline_summary in pipelines:
            pipeline_id = pipeline_summary.pipeline_id
            if not pipeline_id:
                continue

            try:
                pipeline_detail = self._client.pipelines.get(pipeline_id)
            except Exception as e:
                logger.warning("Failed to get pipeline %s: %s", pipeline_id, e)
                continue

            spec = pipeline_detail.spec
            if not spec:
                continue

            # Build API-compatible dict from spec
            config: dict[str, Any] = {
                "pipeline_id": pipeline_id,
                "name": spec.name or "",
                "target": spec.target or None,
                "catalog": spec.catalog or None,
                "libraries": [],
            }
            if spec.libraries:
                for lib in spec.libraries:
                    if lib.notebook:
                        config["libraries"].append(
                            {"notebook": {"path": lib.notebook.path}}
                        )

            info = parse_dlt_pipeline_config(config)

            pipeline_node = Node(
                node_type=NodeType.PIPELINE,
                fqn=pipeline_id,
                name=info.name,
                metadata={
                    "target_catalog": info.target_catalog,
                    "target_schema": info.target_schema,
                },
            )
            self._graph.add_node(pipeline_node)

            # Scan each notebook in the pipeline
            for nb_path in info.notebook_paths:
                # Skip notebooks outside the path prefix when scoped
                if path_prefix and not nb_path.startswith(path_prefix):
                    continue

                nb_node = Node(
                    node_type=NodeType.NOTEBOOK,
                    fqn=nb_path,
                    name=nb_path.split("/")[-1],
                )
                self._graph.add_node(nb_node)

                self._graph.add_edge(Edge(
                    source_id=pipeline_node.id,
                    target_id=nb_node.id,
                    edge_type=EdgeType.CONTAINS,
                ))

                # Fetch notebook source and parse DLT references
                try:
                    export = self._client.workspace.export(nb_path)
                    source = export.content or ""
                    if source:
                        import base64
                        try:
                            source = base64.b64decode(source).decode("utf-8")
                        except Exception:
                            pass  # Already decoded

                        sources, targets = extract_dlt_references_from_code(
                            source,
                            target_catalog=info.target_catalog,
                            target_schema=info.target_schema,
                        )
                        all_refs = sources + targets
                        self._add_table_edges(nb_node, all_refs)
                except Exception as e:
                    logger.warning("Failed to export notebook %s: %s", nb_path, e)

    # ------------------------------------------------------------------
    # Workspace notebook discovery
    # ------------------------------------------------------------------

    def _list_workspace_notebooks(
        self, path_prefix: str = "", max_depth: int = 10
    ) -> list[str]:
        """List notebook paths in the workspace using iterative DFS.

        Args:
            path_prefix: Root path to list from. Defaults to ``/``.
            max_depth: Maximum directory recursion depth.

        Returns:
            List of notebook paths found in the workspace.
        """
        from databricks.sdk.service.workspace import ObjectType

        # Object types to recurse into (Databricks Cloud may use REPO
        # for top-level folders like /Shared and /Repos).
        _CONTAINER_TYPES = {ObjectType.DIRECTORY, ObjectType.REPO}

        root = path_prefix or "/"
        notebook_paths: list[str] = []
        stack: list[tuple[str, int]] = [(root, 0)]

        while stack:
            current_path, depth = stack.pop()
            try:
                objects = list(self._client.workspace.list(current_path))
            except Exception as exc:
                logger.warning("Failed to list workspace path %s: %s", current_path, exc)
                continue

            for obj in objects:
                obj_path = obj.path or ""
                obj_type = obj.object_type

                if obj_type == ObjectType.NOTEBOOK:
                    notebook_paths.append(obj_path)
                elif obj_type in _CONTAINER_TYPES and depth < max_depth:
                    stack.append((obj_path, depth + 1))

        return notebook_paths

    def _scan_workspace_notebooks(self, path_prefix: str = "") -> None:
        """Discover and scan notebooks via workspace listing.

        Lists notebooks under *path_prefix* (or ``/`` when empty), skips any
        notebook that has already been added to the graph by job or pipeline
        scans, and scans the remaining ones for table references.
        """
        notebook_paths = self._list_workspace_notebooks(path_prefix=path_prefix)

        for nb_path in notebook_paths:
            node_id = f"{NodeType.NOTEBOOK.value}::{nb_path}"
            if self._graph.get_node(node_id):
                # Already discovered via a job or pipeline scan — skip
                continue

            refs = self._scan_notebook_path(nb_path)

            nb_node = Node(
                node_type=NodeType.NOTEBOOK,
                fqn=nb_path,
                name=nb_path.split("/")[-1],
            )
            self._graph.add_node(nb_node)
            self._add_table_edges(nb_node, refs)

    # ------------------------------------------------------------------
    # Notebook scanning
    # ------------------------------------------------------------------

    def scan_single_notebook(self, notebook_path: str) -> list[TableReference]:
        """Scan a single notebook and return its table references.

        Also adds the notebook and references to the current graph.
        """
        refs = self._scan_notebook_path(notebook_path)
        nb_node = Node(
            node_type=NodeType.NOTEBOOK,
            fqn=notebook_path,
            name=notebook_path.split("/")[-1],
        )
        self._graph.add_node(nb_node)
        self._add_table_edges(nb_node, refs)
        return refs

    def _scan_notebook_path(self, notebook_path: str) -> list[TableReference]:
        """Fetch and parse a notebook, return table references."""
        try:
            export = self._client.workspace.export(notebook_path)
            source = export.content or ""
            if source:
                import base64
                with contextlib.suppress(Exception):
                    source = base64.b64decode(source).decode("utf-8")

                result = parse_notebook(source)
                return result.table_references
        except Exception as e:
            logger.warning("Failed to scan notebook %s: %s", notebook_path, e)
        return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _add_table_edges(self, source_node: Node, refs: list[TableReference]) -> None:
        """Add table nodes and edges from a source node's references."""
        for ref in refs:
            if not ref.fqn:
                continue

            table_node = Node(
                node_type=NodeType.TABLE,
                fqn=ref.fqn,
                name=ref.table,
            )
            self._graph.add_node(table_node)

            if ref.reference_type == "writes_to":
                self._graph.add_edge(Edge(
                    source_id=source_node.id,
                    target_id=table_node.id,
                    edge_type=EdgeType.WRITES_TO,
                ))
            else:
                self._graph.add_edge(Edge(
                    source_id=table_node.id,
                    target_id=source_node.id,
                    edge_type=EdgeType.READS_FROM,
                ))
