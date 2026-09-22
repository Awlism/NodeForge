"""Persistent storage for NodeForge service metadata."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from freemesh.service_requirements import (
    ServiceRequirements,
)


class ServiceMetadataStore:
    """SQLite-backed persistent storage for service metadata."""

    def __init__(
        self,
        database_path: str,
    ) -> None:
        if not database_path:
            raise ValueError(
                "database_path is required"
            )

        self.database_path = database_path

        if database_path != ":memory:":
            Path(database_path).parent.mkdir(
                parents=True,
                exist_ok=True,
            )

        self._connection = sqlite3.connect(
            database_path,
            timeout=30.0,
            isolation_level=None,
        )

        self._connection.row_factory = (
            sqlite3.Row
        )

        self._configure_database()
        self._initialize()

    def _configure_database(self) -> None:
        """Configure SQLite for reliable metadata persistence."""

        self._connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        self._connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        if self.database_path != ":memory:":
            self._connection.execute(
                "PRAGMA journal_mode = WAL"
            )

        self._connection.execute(
            "PRAGMA synchronous = NORMAL"
        )

    def _initialize(self) -> None:
        """Create the metadata schema."""

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

    def save(
        self,
        service_id: str,
        node_id: Optional[str],
        pid: Optional[int],
        status: str,
        health: str,
        command: Optional[str],
        requirements: ServiceRequirements,
        restart_attempts: int,
        updated_at: Optional[datetime] = None,
    ) -> None:
        """Save or update service metadata."""

        if (
            not isinstance(
                service_id,
                str,
            )
            or not service_id.strip()
        ):
            raise ValueError(
                "service_id cannot be empty"
            )

        if (
            node_id is not None
            and (
                not isinstance(
                    node_id,
                    str,
                )
                or not node_id.strip()
            )
        ):
            raise ValueError(
                "node_id must be a non-empty string or None"
            )

        if (
            pid is not None
            and (
                not isinstance(
                    pid,
                    int,
                )
                or isinstance(
                    pid,
                    bool,
                )
                or pid <= 0
            )
        ):
            raise ValueError(
                "pid must be a positive integer or None"
            )

        if not isinstance(
            status,
            str,
        ) or not status.strip():
            raise ValueError(
                "status cannot be empty"
            )

        if not isinstance(
            health,
            str,
        ) or not health.strip():
            raise ValueError(
                "health cannot be empty"
            )

        if (
            command is not None
            and not isinstance(
                command,
                str,
            )
        ):
            raise TypeError(
                "command must be a string or None"
            )

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be "
                "a ServiceRequirements instance"
            )

        if (
            not isinstance(
                restart_attempts,
                int,
            )
            or isinstance(
                restart_attempts,
                bool,
            )
            or restart_attempts < 0
        ):
            raise ValueError(
                "restart_attempts must be "
                "a non-negative integer"
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
            requirements.to_dict(),
            separators=(",", ":"),
            sort_keys=True,
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
                updated_at =
                    excluded.updated_at
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

    def list_all(
        self,
    ) -> list[dict[str, Any]]:
        """Return all service metadata."""

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

    def delete(
        self,
        service_id: str,
    ) -> bool:
        """Delete service metadata."""

        cursor = self._connection.execute(
            """
            DELETE FROM service_metadata
            WHERE service_id = ?
            """,
            (service_id,),
        )

        return cursor.rowcount > 0

    def exists(
        self,
        service_id: str,
    ) -> bool:
        """Return whether metadata exists."""

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
            SELECT COUNT(*) AS count
            FROM service_metadata
            """
        ).fetchone()

        return int(row["count"])

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
            "restart_attempts": int(
                row["restart_attempts"]
            ),
            "updated_at": updated_at,
        }