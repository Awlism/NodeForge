"""Persistent storage for NodeForge service metadata."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from freemesh.service_requirements import ServiceRequirements


class ServiceMetadataStore:
    """SQLite-backed persistent storage for service metadata."""

    def __init__(self, database_path: str) -> None:
        self.database_path = database_path

        if database_path != ":memory:":
            Path(database_path).parent.mkdir(
                parents=True,
                exist_ok=True,
            )

        self._connection = sqlite3.connect(
            database_path
        )
        self._connection.row_factory = sqlite3.Row

        self._initialize()

    def _initialize(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS service_metadata (
                service_id TEXT PRIMARY KEY,
                node_id TEXT,
                pid INTEGER,
                status TEXT NOT NULL,
                health TEXT NOT NULL,
                command TEXT,
                requirements_json TEXT NOT NULL,
                restart_attempts INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        self._connection.commit()

    def save(
        self,
        service_id: str,
        node_id: str,
        pid: Optional[int],
        status: str,
        health: str,
        command: Optional[str],
        requirements: ServiceRequirements,
        restart_attempts: int,
        updated_at: Optional[datetime] = None,
    ) -> None:
        """Save or update service metadata."""

        if not service_id:
            raise ValueError(
                "service_id cannot be empty"
            )

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be "
                "a ServiceRequirements instance"
            )

        if restart_attempts < 0:
            raise ValueError(
                "restart_attempts cannot be negative"
            )

        if updated_at is None:
            updated_at = datetime.now(
                timezone.utc
            )

        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(
                tzinfo=timezone.utc
            )

        requirements_json = json.dumps(
            requirements.to_dict()
        )

        self._connection.execute(
            """
            INSERT INTO service_metadata (
                service_id,
                node_id,
                pid,
                status,
                health,
                command,
                requirements_json,
                restart_attempts,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(service_id)
            DO UPDATE SET
                node_id = excluded.node_id,
                pid = excluded.pid,
                status = excluded.status,
                health = excluded.health,
                command = excluded.command,
                requirements_json =
                    excluded.requirements_json,
                restart_attempts =
                    excluded.restart_attempts,
                updated_at = excluded.updated_at
            """,
            (
                service_id,
                node_id,
                pid,
                status,
                health,
                command,
                requirements_json,
                restart_attempts,
                updated_at.isoformat(),
            ),
        )

        self._connection.commit()

    def get(
        self,
        service_id: str,
    ) -> Optional[dict[str, Any]]:
        """Return metadata for a service."""

        row = self._connection.execute(
            """
            SELECT
                service_id,
                node_id,
                pid,
                status,
                health,
                command,
                requirements_json,
                restart_attempts,
                updated_at
            FROM service_metadata
            WHERE service_id = ?
            """,
            (service_id,),
        ).fetchone()

        if row is None:
            return None

        return self._row_to_metadata(row)

    def list_all(self) -> list[dict[str, Any]]:
        """Return all service metadata ordered by service ID."""

        rows = self._connection.execute(
            """
            SELECT
                service_id,
                node_id,
                pid,
                status,
                health,
                command,
                requirements_json,
                restart_attempts,
                updated_at
            FROM service_metadata
            ORDER BY service_id
            """
        ).fetchall()

        return [
            self._row_to_metadata(row)
            for row in rows
        ]

    def delete(self, service_id: str) -> bool:
        """Delete service metadata.

        Returns True if a record was deleted.
        """

        cursor = self._connection.execute(
            """
            DELETE FROM service_metadata
            WHERE service_id = ?
            """,
            (service_id,),
        )

        self._connection.commit()

        return cursor.rowcount > 0

    def exists(self, service_id: str) -> bool:
        """Return whether metadata exists for a service."""

        row = self._connection.execute(
            """
            SELECT 1
            FROM service_metadata
            WHERE service_id = ?
            LIMIT 1
            """,
            (service_id,),
        ).fetchone()

        return row is not None

    def count(self) -> int:
        """Return the number of stored services."""

        row = self._connection.execute(
            """
            SELECT COUNT(*)
            FROM service_metadata
            """
        ).fetchone()

        return int(row[0])

    def close(self) -> None:
        """Close the SQLite connection."""

        self._connection.close()

    def _row_to_metadata(
        self,
        row: sqlite3.Row,
    ) -> dict[str, Any]:
        """Convert a database row into metadata."""

        updated_at = datetime.fromisoformat(
            row["updated_at"]
        )

        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(
                tzinfo=timezone.utc
            )

        requirements_payload = json.loads(
            row["requirements_json"]
        )

        return {
            "service_id": row["service_id"],
            "node_id": row["node_id"],
            "pid": row["pid"],
            "status": row["status"],
            "health": row["health"],
            "command": row["command"],
            "requirements": (
                ServiceRequirements.from_dict(
                    requirements_payload
                )
            ),
            "restart_attempts": row[
                "restart_attempts"
            ],
            "updated_at": updated_at,
        }