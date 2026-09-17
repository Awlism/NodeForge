"""Failure and recovery coordination for NodeForge."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FailureRecord:
    """Record of a service failure."""

    service_id: str
    node_id: str
    status: str
    reason: Optional[str] = None
    restart_attempts: int = 0


class FailureManager:
    """Track service failures and determine recovery actions."""

    def __init__(self) -> None:
        self._failures: dict[str, FailureRecord] = {}

    def record_failure(
        self,
        service_id: str,
        node_id: str,
        status: str = "crashed",
        reason: Optional[str] = None,
        restart_attempts: int = 0,
    ) -> FailureRecord:
        if not service_id:
            raise ValueError("service_id is required")

        if not node_id:
            raise ValueError("node_id is required")

        record = FailureRecord(
            service_id=service_id,
            node_id=node_id,
            status=status,
            reason=reason,
            restart_attempts=restart_attempts,
        )

        self._failures[service_id] = record

        return record

    def get_failure(
        self,
        service_id: str,
    ) -> Optional[FailureRecord]:
        return self._failures.get(service_id)

    def clear_failure(self, service_id: str) -> None:
        self._failures.pop(service_id, None)

    def list_failures(self) -> list[FailureRecord]:
        return list(self._failures.values())

    def should_failover(
        self,
        service_id: str,
        max_restart_attempts: int,
    ) -> bool:
        failure = self._failures.get(service_id)

        if failure is None:
            return False

        return failure.restart_attempts >= max_restart_attempts