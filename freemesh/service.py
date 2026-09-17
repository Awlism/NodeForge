"""Service model and lifecycle states for NodeForge."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from freemesh.service_requirements import ServiceRequirements


class ServiceStatus(str, Enum):
    """Lifecycle states of a NodeForge service."""

    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"
    FAILED = "failed"


class ServiceHealth(str, Enum):
    """Health states of a NodeForge service."""

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
        """Mark the service as starting."""

        self.status = ServiceStatus.STARTING
        self.health = ServiceHealth.UNKNOWN

    def mark_running(
        self,
        pid: int,
        node_id: Optional[str] = None,
    ) -> None:
        """Mark the service as running."""

        self.status = ServiceStatus.RUNNING
        self.pid = pid

        if node_id is not None:
            self.node_id = node_id

    def mark_healthy(self) -> None:
        """Mark the service as healthy."""

        if self.status != ServiceStatus.RUNNING:
            raise RuntimeError(
                "Only a running service can be marked healthy"
            )

        self.health = ServiceHealth.HEALTHY

    def mark_unhealthy(self) -> None:
        """Mark the service as unhealthy."""

        if self.status != ServiceStatus.RUNNING:
            raise RuntimeError(
                "Only a running service can be marked unhealthy"
            )

        self.health = ServiceHealth.UNHEALTHY

    def mark_crashed(self) -> None:
        """Mark the service as crashed."""

        self.status = ServiceStatus.CRASHED
        self.health = ServiceHealth.CRASHED

    def mark_stopping(self) -> None:
        """Mark the service as stopping."""

        self.status = ServiceStatus.STOPPING
        self.health = ServiceHealth.UNKNOWN

    def mark_stopped(self) -> None:
        """Mark the service as stopped."""

        self.status = ServiceStatus.STOPPED
        self.health = ServiceHealth.UNKNOWN
        self.pid = None
        self.node_id = None

    def mark_failed(self) -> None:
        """Mark the service as failed."""

        self.status = ServiceStatus.FAILED
        self.health = ServiceHealth.UNHEALTHY
        self.pid = None

    def is_healthy(self) -> bool:
        """Return whether the service is currently healthy."""

        return (
            self.status == ServiceStatus.RUNNING
            and self.health == ServiceHealth.HEALTHY
        )