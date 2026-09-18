"""Persistent resource registry for the NodeForge controller."""

import sqlite3
from typing import Dict, List, Optional

from freemesh.node.resources import NodeResources


class ResourceRegistry:
    """Store and manage node resource information.

    Resource information is kept in memory for fast access and
    persisted to SQLite so it survives controller restarts.
    """

    def __init__(
        self,
        database_path: str = ":memory:",
    ) -> None:
        """Initialize the resource registry.

        Args:
            database_path:
                SQLite database path.
                Defaults to ':memory:' for backward compatibility.
        """

        self._resources: Dict[str, NodeResources] = {}

        self._database_path = database_path
        self._connection = sqlite3.connect(
            database_path,
            timeout=30.0,
        )

        self._connection.row_factory = sqlite3.Row

        self._create_tables()
        self._load_from_store()

    def _create_tables(self) -> None:
        """Create the persistent resource table."""

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS node_resources (
                node_id TEXT PRIMARY KEY,
                cpu_cores REAL NOT NULL,
                cpu_usage_percent REAL NOT NULL,
                memory_total_mb INTEGER NOT NULL,
                memory_used_mb INTEGER NOT NULL,
                disk_total_gb REAL NOT NULL,
                disk_used_gb REAL NOT NULL,
                running_services INTEGER NOT NULL
            )
            """
        )

        self._connection.commit()

    def _persist_resources(
        self,
        node_id: str,
        resources: NodeResources,
    ) -> None:
        """Persist resource information for one node."""

        self._connection.execute(
            """
            INSERT INTO node_resources (
                node_id,
                cpu_cores,
                cpu_usage_percent,
                memory_total_mb,
                memory_used_mb,
                disk_total_gb,
                disk_used_gb,
                running_services
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                cpu_cores = excluded.cpu_cores,
                cpu_usage_percent =
                    excluded.cpu_usage_percent,
                memory_total_mb =
                    excluded.memory_total_mb,
                memory_used_mb =
                    excluded.memory_used_mb,
                disk_total_gb =
                    excluded.disk_total_gb,
                disk_used_gb =
                    excluded.disk_used_gb,
                running_services =
                    excluded.running_services
            """,
            (
                node_id,
                resources.cpu_cores,
                resources.cpu_usage_percent,
                resources.memory_total_mb,
                resources.memory_used_mb,
                resources.disk_total_gb,
                resources.disk_used_gb,
                resources.running_services,
            ),
        )

        self._connection.commit()

    def _load_from_store(self) -> None:
        """Load persisted resource information."""

        rows = self._connection.execute(
            """
            SELECT
                node_id,
                cpu_cores,
                cpu_usage_percent,
                memory_total_mb,
                memory_used_mb,
                disk_total_gb,
                disk_used_gb,
                running_services
            FROM node_resources
            """
        ).fetchall()

        for row in rows:
            self._resources[row["node_id"]] = (
                NodeResources(
                    cpu_cores=row["cpu_cores"],
                    cpu_usage_percent=(
                        row["cpu_usage_percent"]
                    ),
                    memory_total_mb=(
                        row["memory_total_mb"]
                    ),
                    memory_used_mb=(
                        row["memory_used_mb"]
                    ),
                    disk_total_gb=(
                        row["disk_total_gb"]
                    ),
                    disk_used_gb=(
                        row["disk_used_gb"]
                    ),
                    running_services=(
                        row["running_services"]
                    ),
                )
            )

    def register_resources(
        self,
        node_id: str,
        resources: NodeResources,
    ) -> NodeResources:
        """Register or replace resource information."""

        if not node_id:
            raise ValueError(
                "node_id is required"
            )

        if not isinstance(
            resources,
            NodeResources,
        ):
            raise TypeError(
                "resources must be a NodeResources instance"
            )

        self._resources[node_id] = resources

        self._persist_resources(
            node_id,
            resources,
        )

        return resources

    def get_resources(
        self,
        node_id: str,
    ) -> Optional[NodeResources]:
        """Return resource information for a node."""

        return self._resources.get(node_id)

    def update_resources(
        self,
        node_id: str,
        resources: NodeResources,
    ) -> NodeResources:
        """Update resource information for an existing node."""

        if not node_id:
            raise ValueError(
                "node_id is required"
            )

        if not isinstance(
            resources,
            NodeResources,
        ):
            raise TypeError(
                "resources must be a NodeResources instance"
            )

        if node_id not in self._resources:
            raise KeyError(
                f"Resources for node {node_id} not found"
            )

        self._resources[node_id] = resources

        self._persist_resources(
            node_id,
            resources,
        )

        return resources

    def remove_resources(
        self,
        node_id: str,
    ) -> Optional[NodeResources]:
        """Remove resource information for a node."""

        resources = self._resources.pop(
            node_id,
            None,
        )

        if resources is not None:
            self._connection.execute(
                """
                DELETE FROM node_resources
                WHERE node_id = ?
                """,
                (node_id,),
            )

            self._connection.commit()

        return resources

    def list_resources(
        self,
    ) -> List[tuple[str, NodeResources]]:
        """Return resource information for all nodes."""

        return list(
            self._resources.items()
        )

    def list_node_ids(self) -> List[str]:
        """Return IDs of all nodes with resources."""

        return list(
            self._resources.keys()
        )

    def has_resources(
        self,
        node_id: str,
    ) -> bool:
        """Return whether resource information exists."""

        return node_id in self._resources

    def node_count(self) -> int:
        """Return the number of nodes with resources."""

        return len(self._resources)

    def clear(self) -> None:
        """Remove all resources from memory and storage."""

        self._resources.clear()

        self._connection.execute(
            "DELETE FROM node_resources"
        )

        self._connection.commit()

    def close(self) -> None:
        """Close the SQLite connection."""

        self._connection.close()