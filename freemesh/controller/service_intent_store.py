"""Persistent storage for NodeForge service intents."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


class ServiceIntentStore:
    """Persist service intents using resilient SQLite settings."""

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
        """Configure SQLite for reliable controller state."""

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
        """Create the storage schema."""

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS service_intents (
                service_id TEXT PRIMARY KEY,
                desired_state TEXT NOT NULL,
                command TEXT,
                requirements_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def save(
        self,
        intent: ServiceIntent,
    ) -> ServiceIntent:
        """Insert or replace a service intent."""

        if not isinstance(
            intent,
            ServiceIntent,
        ):
            raise TypeError(
                "intent must be a ServiceIntent instance"
            )

        requirements_json = json.dumps(
            intent.requirements.to_dict(),
            separators=(",", ":"),
            sort_keys=True,
        )

        updated_at = intent.updated_at

        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(
                tzinfo=timezone.utc
            )

        self._connection.execute(
            """
            INSERT INTO service_intents (
                service_id,
                desired_state,
                command,
                requirements_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(service_id)
            DO UPDATE SET
                desired_state = excluded.desired_state,
                command = excluded.command,
                requirements_json = excluded.requirements_json,
                updated_at = excluded.updated_at
            """,
            (
                intent.service_id,
                intent.desired_state.value,
                intent.command,
                requirements_json,
                updated_at.isoformat(),
            ),
        )

        return intent

    def get(
        self,
        service_id: str,
    ) -> ServiceIntent | None:
        """Load one service intent."""

        row = self._connection.execute(
            """
            SELECT
                service_id,
                desired_state,
                command,
                requirements_json,
                updated_at
            FROM service_intents
            WHERE service_id = ?
            """,
            (service_id,),
        ).fetchone()

        if row is None:
            return None

        return self._row_to_intent(row)

    def list_all(
        self,
    ) -> list[ServiceIntent]:
        """Load all persisted service intents."""

        rows = self._connection.execute(
            """
            SELECT
                service_id,
                desired_state,
                command,
                requirements_json,
                updated_at
            FROM service_intents
            ORDER BY service_id
            """
        ).fetchall()

        return [
            self._row_to_intent(row)
            for row in rows
        ]

    def delete(
        self,
        service_id: str,
    ) -> bool:
        """Delete an intent."""

        cursor = self._connection.execute(
            """
            DELETE FROM service_intents
            WHERE service_id = ?
            """,
            (service_id,),
        )

        return cursor.rowcount > 0

    def exists(
        self,
        service_id: str,
    ) -> bool:
        """Return whether an intent exists."""

        row = self._connection.execute(
            """
            SELECT 1
            FROM service_intents
            WHERE service_id = ?
            LIMIT 1
            """,
            (service_id,),
        ).fetchone()

        return row is not None

    def count(self) -> int:
        """Return the number of persisted intents."""

        row = self._connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM service_intents
            """
        ).fetchone()

        return int(row["count"])

    def close(self) -> None:
        """Close the SQLite connection."""

        self._connection.close()

    def _row_to_intent(
        self,
        row: sqlite3.Row,
    ) -> ServiceIntent:
        """Convert a database row into a ServiceIntent."""

        requirements_data = json.loads(
            row["requirements_json"]
        )

        updated_at = datetime.fromisoformat(
            row["updated_at"]
        )

        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(
                tzinfo=timezone.utc
            )

        return ServiceIntent(
            service_id=row["service_id"],
            desired_state=DesiredState(
                row["desired_state"]
            ),
            command=row["command"],
            requirements=ServiceRequirements.from_dict(
                requirements_data
            ),
            updated_at=updated_at,
        )