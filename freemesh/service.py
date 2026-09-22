"""Service model and lifecycle states for NodeForge."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from freemesh.service_requirements import ServiceRequirements


class ServiceStatus(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"
    FAILED = "failed"


class ServiceHealth(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    CRASHED = "crashed"


@dataclass
class Service:
    """Represent a NodeForge service and its runtime state."""

    service_id: str
    command: str
    status: ServiceStatus = ServiceStatus.STOPPED
    health: ServiceHealth = ServiceHealth.UNKNOWN
    pid: Optional[int] = None
    restart_attempts: int = 0
    max_restart_attempts: int = 3
    requirements: ServiceRequirements = field(
        default_factory=ServiceRequirements
    )
    node_id: Optional[str] = None

    def mark_starting(self) -> None:
        self.status = ServiceStatus.STARTING
        self.health = ServiceHealth.UNKNOWN
        self.pid = None

    def mark_running(
        self,
        pid: int,
        node_id: Optional[str] = None,
    ) -> None:
        if not isinstance(pid, int) or pid <= 0:
            raise ValueError("pid must be a positive integer")

        self.status = ServiceStatus.RUNNING
        self.health = ServiceHealth.UNKNOWN
        self.pid = pid

        if node_id is not None:
            self.node_id = node_id

    def mark_healthy(self) -> None:
        if self.status != ServiceStatus.RUNNING:
            raise RuntimeError(
                "Only a running service can be marked healthy"
            )

        self.health = ServiceHealth.HEALTHY

    def mark_unhealthy(self) -> None:
        if self.status != ServiceStatus.RUNNING:
            raise RuntimeError(
                "Only a running service can be marked unhealthy"
            )

        self.health = ServiceHealth.UNHEALTHY

    def mark_crashed(self) -> None:
        self.status = ServiceStatus.CRASHED
        self.health = ServiceHealth.CRASHED

    def mark_stopping(self) -> None:
        self.status = ServiceStatus.STOPPING
        self.health = ServiceHealth.UNKNOWN

    def mark_stopped(self) -> None:
        self.status = ServiceStatus.STOPPED
        self.health = ServiceHealth.UNKNOWN
        self.pid = None
        self.node_id = None

    def mark_failed(self) -> None:
        self.status = ServiceStatus.FAILED
        self.health = ServiceHealth.UNHEALTHY
        self.pid = None

    def is_healthy(self) -> bool:
        return (
            self.status == ServiceStatus.RUNNING
            and self.health == ServiceHealth.HEALTHY
        )