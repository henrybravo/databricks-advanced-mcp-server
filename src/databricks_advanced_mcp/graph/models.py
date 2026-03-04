"""Data models for the dependency graph.

Defines nodes, edges, and a DependencyGraph wrapper around NetworkX DiGraph.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import networkx as nx


class NodeType(StrEnum):
    """Types of nodes in the dependency graph."""

    TABLE = "table"
    COLUMN = "column"
    NOTEBOOK = "notebook"
    JOB = "job"
    PIPELINE = "pipeline"
    QUERY = "query"


class EdgeType(StrEnum):
    """Types of edges/relationships in the dependency graph."""

    READS_FROM = "reads_from"
    WRITES_TO = "writes_to"
    DEPENDS_ON = "depends_on"
    TRIGGERS = "triggers"
    CONTAINS = "contains"


@dataclass
class Node:
    """A node in the dependency graph."""

    node_type: NodeType
    fqn: str  # Fully-qualified name (unique identifier)
    name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        """Unique ID for the node (type::fqn)."""
        return f"{self.node_type.value}::{self.fqn}"


@dataclass
class Edge:
    """An edge in the dependency graph."""

    source_id: str  # Node.id of the source
    target_id: str  # Node.id of the target
    edge_type: EdgeType
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphSummary:
    """Summary statistics of the dependency graph."""

    node_count: int = 0
    edge_count: int = 0
    node_counts_by_type: dict[str, int] = field(default_factory=dict)
    edge_counts_by_type: dict[str, int] = field(default_factory=dict)
    root_nodes: list[str] = field(default_factory=list)  # Nodes with no incoming edges
    leaf_nodes: list[str] = field(default_factory=list)  # Nodes with no outgoing edges


class DependencyGraph:
    """Wrapper around NetworkX DiGraph for dependency tracking.

    Nodes are keyed by their `Node.id` (type::fqn). Node data is stored
    as node attributes. Edges carry their EdgeType and metadata.
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph[str] = nx.DiGraph()

    @property
    def graph(self) -> nx.DiGraph[str]:
        """Access the underlying NetworkX graph."""
        return self._graph

    def add_node(self, node: Node) -> None:
        """Add or update a node in the graph."""
        self._graph.add_node(
            node.id,
            node_type=node.node_type.value,
            fqn=node.fqn,
            name=node.name,
            metadata=node.metadata,
        )

    def add_edge(self, edge: Edge) -> None:
        """Add an edge between two nodes."""
        self._graph.add_edge(
            edge.source_id,
            edge.target_id,
            edge_type=edge.edge_type.value,
            metadata=edge.metadata,
        )

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Get node data by ID."""
        if node_id in self._graph:
            return dict(self._graph.nodes[node_id])
        return None

    def get_upstream(self, node_id: str, depth: int = -1) -> list[str]:
        """Get all upstream (ancestor) node IDs.

        Args:
            node_id: Starting node.
            depth: Max depth to traverse. -1 for unlimited.
        """
        if node_id not in self._graph:
            return []
        if depth == -1:
            return list(nx.ancestors(self._graph, node_id))
        # BFS with depth limit
        visited: set[str] = set()
        current = {node_id}
        for _ in range(depth):
            next_level: set[str] = set()
            for n in current:
                for pred in self._graph.predecessors(n):
                    if pred not in visited:
                        visited.add(pred)
                        next_level.add(pred)
            current = next_level
            if not current:
                break
        return list(visited)

    def get_downstream(self, node_id: str, depth: int = -1) -> list[str]:
        """Get all downstream (descendant) node IDs.

        Args:
            node_id: Starting node.
            depth: Max depth to traverse. -1 for unlimited.
        """
        if node_id not in self._graph:
            return []
        if depth == -1:
            return list(nx.descendants(self._graph, node_id))
        visited: set[str] = set()
        current = {node_id}
        for _ in range(depth):
            next_level: set[str] = set()
            for n in current:
                for succ in self._graph.successors(n):
                    if succ not in visited:
                        visited.add(succ)
                        next_level.add(succ)
            current = next_level
            if not current:
                break
        return list(visited)

    def get_path(self, source_id: str, target_id: str) -> list[str]:
        """Get the shortest path between two nodes."""
        try:
            return list(nx.shortest_path(self._graph, source_id, target_id))
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

    def summary(self) -> GraphSummary:
        """Compute summary statistics of the graph."""
        node_counts: dict[str, int] = {}
        for _, data in self._graph.nodes(data=True):
            nt = data.get("node_type", "unknown")
            node_counts[nt] = node_counts.get(nt, 0) + 1

        edge_counts: dict[str, int] = {}
        for _, _, data in self._graph.edges(data=True):
            et = data.get("edge_type", "unknown")
            edge_counts[et] = edge_counts.get(et, 0) + 1

        roots = [n for n in self._graph.nodes() if self._graph.in_degree(n) == 0]
        leaves = [n for n in self._graph.nodes() if self._graph.out_degree(n) == 0]

        return GraphSummary(
            node_count=self._graph.number_of_nodes(),
            edge_count=self._graph.number_of_edges(),
            node_counts_by_type=node_counts,
            edge_counts_by_type=edge_counts,
            root_nodes=roots[:50],  # Cap to avoid oversized responses
            leaf_nodes=leaves[:50],
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize graph to a dict for JSON output."""
        return {
            "nodes": [
                {"id": n, **data} for n, data in self._graph.nodes(data=True)
            ],
            "edges": [
                {"source": u, "target": v, **data}
                for u, v, data in self._graph.edges(data=True)
            ],
        }

    def clear(self) -> None:
        """Remove all nodes and edges."""
        self._graph.clear()
