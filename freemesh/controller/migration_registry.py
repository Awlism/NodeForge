"""Migration state registry for NodeForge."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class MigrationRecord:
    service_id: str
    source_node_id: str
    target_node_id: str
    status: str
    error: Optional[str] = None
    pid: Optional[int] = None
    created_at: datetime = None

    def __post_init__(self) -> None:
        if self.created_at is None:
            self.created_at = datetime.now(
                timezone.utc
            )


class MigrationRegistry:
    """Track migration attempts and their final state."""

    def __init__(self) -> None:
        self._records: dict[
            str,
            MigrationRecord,
        ] = {}

    def start(
        self,
        service_id: str,
        source_node_id: str,
        target_node_id: str,
    ) -> MigrationRecord:
        record = MigrationRecord(
            service_id=service_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            status="started",
        )

        self._records[service_id] = record

        return record

    def complete(
        self,
        service_id: str,
        pid: Optional[int] = None,
    ) -> MigrationRecord:
        record = self._require(
            service_id
        )

        record.status = "migrated"
        record.pid = pid
        record.error = None

        return record

    def fail(
        self,
        service_id: str,
        error: str,
        status: str = "failed",
    ) -> MigrationRecord:
        record = self._require(
            service_id
        )

        record.status = status
        record.error = error

        return record

    def get(
        self,
        service_id: str,
    ) -> Optional[MigrationRecord]:
        return self._records.get(
            service_id
        )

    def list_records(
        self,
    ) -> list[MigrationRecord]:
        return list(
            self._records.values()
        )

    def clear(
        self,
        service_id: str,
    ) -> None:
        self._records.pop(
            service_id,
            None,
        )

    def _require(
        self,
        service_id: str,
    ) -> MigrationRecord:
        record = self._records.get(
            service_id
        )

        if record is None:
            raise KeyError(
                f"Migration {service_id} not found"
            )

        return record