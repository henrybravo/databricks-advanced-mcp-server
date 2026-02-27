"""In-memory cache for the dependency graph.

Provides a singleton cache with TTL-based staleness checking and
explicit invalidation support.
"""

from __future__ import annotations

import time
from typing import Any

from databricks_advanced_mcp.graph.models import DependencyGraph, GraphSummary


class GraphCache:
    """Singleton in-memory cache for the DependencyGraph."""

    _instance: GraphCache | None = None

    def __init__(self) -> None:
        self._graph: DependencyGraph | None = None
        self._timestamp: float = 0.0
        self._ttl: int = 3600  # Default 1 hour

    @classmethod
    def get_instance(cls) -> GraphCache:
        """Return the singleton cache instance."""
        if cls._instance is None:
            cls._instance = GraphCache()
        return cls._instance

    @property
    def graph(self) -> DependencyGraph | None:
        """Return the cached graph, or None if not set."""
        return self._graph

    @property
    def timestamp(self) -> float:
        """Return the timestamp of the last cache update."""
        return self._timestamp

    @property
    def ttl(self) -> int:
        """Return the TTL in seconds."""
        return self._ttl

    @ttl.setter
    def ttl(self, value: int) -> None:
        self._ttl = value

    def is_stale(self) -> bool:
        """Check if the cache is stale (older than TTL)."""
        if self._graph is None:
            return True
        return (time.time() - self._timestamp) > self._ttl

    def is_valid(self) -> bool:
        """Check if the cache has a valid, non-stale graph."""
        return self._graph is not None and not self.is_stale()

    def set(self, graph: DependencyGraph) -> None:
        """Store a graph in the cache with current timestamp."""
        self._graph = graph
        self._timestamp = time.time()

    def invalidate(self) -> None:
        """Invalidate the cache, forcing a rebuild on next access."""
        self._graph = None
        self._timestamp = 0.0

    def get_or_none(self) -> DependencyGraph | None:
        """Return the graph if valid, None otherwise."""
        if self.is_valid():
            return self._graph
        return None

    def get_or_stale(self) -> DependencyGraph | None:
        """Return the graph even if stale, None only when no graph exists."""
        return self._graph

    def summary(self) -> dict[str, Any]:
        """Return cache status and graph summary."""
        result: dict[str, Any] = {
            "has_graph": self._graph is not None,
            "is_stale": self.is_stale(),
            "cache_age_seconds": round(time.time() - self._timestamp, 1) if self._timestamp else None,
            "ttl_seconds": self._ttl,
        }

        if self._graph is not None:
            gs: GraphSummary = self._graph.summary()
            result["graph_summary"] = {
                "node_count": gs.node_count,
                "edge_count": gs.edge_count,
                "node_counts_by_type": gs.node_counts_by_type,
                "edge_counts_by_type": gs.edge_counts_by_type,
                "root_count": len(gs.root_nodes),
                "leaf_count": len(gs.leaf_nodes),
            }

        return result

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (mainly for testing)."""
        cls._instance = None
