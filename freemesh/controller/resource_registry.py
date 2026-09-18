"""Persistent and freshness-aware resource registry."""

import sqlite3
import time
from typing import Dict, List, Optional

from freemesh.node.resources import NodeResources


class ResourceRegistry:
    """Store node resources with persistence and freshness tracking."""

    def __init__(
        self,
        database_path: str = ":memory:",
        freshness_timeout_seconds: float = 30.0,
    ) -> None:
        """Initialize the resource registry.

        Args:
            database_path:
                SQLite database path.

            freshness_timeout_seconds:
                Maximum age in seconds for a resource report to be
                considered fresh.
        """

        if freshness_timeout_seconds <= 0:
            raise ValueError(
                "freshness_timeout_seconds must be greater than 0"
            )

        self._resources: Dict[str, NodeResources] = {}
        self._resource_updated_at: Dict[str, float] = {}

        self._database_path = database_path
        self._freshness_timeout_seconds = (
            freshness_timeout_seconds
        )

        self._connection = sqlite3.connect(
            database_path,
            timeout=30.0,
        )

        self._connection.row_factory = sqlite3.Row

        self._create_tables()
        self._load_from_store()

    def _create_tables(self) -> None:
        """Create or migrate the persistent resource table."""

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

        columns = self._connection.execute(
            """
            PRAGMA table_info(node_resources)
            """
        ).fetchall()

        column_names = {
            column["name"]
            for column in columns
        }

        if "updated_at" not in column_names:
            self._connection.execute(
                """
                ALTER TABLE node_resources
                ADD COLUMN updated_at REAL
                """
            )

            self._connection.execute(
                """
                UPDATE node_resources
                SET updated_at = ?
                WHERE updated_at IS NULL
                """,
                (0.0,),
            )

        self._connection.commit()

    def _persist_resources(
        self,
        node_id: str,
        resources: NodeResources,
        updated_at: float,
    ) -> None:
        """Persist resource information and its timestamp."""

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
                running_services,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    excluded.running_services,
                updated_at =
                    excluded.updated_at
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
                updated_at,
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
                running_services,
                updated_at
            FROM node_resources
            """
        ).fetchall()

        for row in rows:
            node_id = row["node_id"]

            self._resources[node_id] = NodeResources(
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

            self._resource_updated_at[node_id] = (
                float(row["updated_at"] or 0.0)
            )

    def _is_fresh(
        self,
        node_id: str,
        now: Optional[float] = None,
    ) -> bool:
        """Return whether a node's resource report is fresh."""

        updated_at = self._resource_updated_at.get(
            node_id
        )

        if updated_at is None:
            return False

        if now is None:
            now = time.time()

        age = now - updated_at

        return (
            age <= self._freshness_timeout_seconds
        )

    def is_fresh(
        self,
        node_id: str,
    ) -> bool:
        """Return whether a node's resources are currently fresh."""

        if node_id not in self._resources:
            return False

        return self._is_fresh(node_id)

    def get_resource_age_seconds(
        self,
        node_id: str,
    ) -> Optional[float]:
        """Return the age of a resource report in seconds."""

        updated_at = self._resource_updated_at.get(
            node_id
        )

        if updated_at is None:
            return None

        return max(
            0.0,
            time.time() - updated_at,
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

        updated_at = time.time()

        self._resources[node_id] = resources
        self._resource_updated_at[node_id] = (
            updated_at
        )

        self._persist_resources(
            node_id,
            resources,
            updated_at,
        )

        return resources

    def get_resources(
        self,
        node_id: str,
        allow_stale: bool = False,
    ) -> Optional[NodeResources]:
        """Return node resources.

        By default only fresh resources are returned.

        ``allow_stale=True`` is intended for inspection,
        diagnostics, recovery, or persistence tests.
        Scheduling should use the default behavior.
        """

        resources = self._resources.get(
            node_id
        )

        if resources is None:
            return None

        if allow_stale:
            return resources

        if not self._is_fresh(node_id):
            return None

        return resources

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

        updated_at = time.time()

        self._resources[node_id] = resources
        self._resource_updated_at[node_id] = (
            updated_at
        )

        self._persist_resources(
            node_id,
            resources,
            updated_at,
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

        self._resource_updated_at.pop(
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
        """Return all stored resource information."""

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
        """Return whether stored resource information exists."""

        return node_id in self._resources

    def node_count(self) -> int:
        """Return the number of nodes with resources."""

        return len(self._resources)

    def clear(self) -> None:
        """Remove all resources from memory and storage."""

        self._resources.clear()
        self._resource_updated_at.clear()

        self._connection.execute(
            "DELETE FROM node_resources"
        )

        self._connection.commit()

    def close(self) -> None:
        """Close the SQLite connection."""

        self._connection.close()