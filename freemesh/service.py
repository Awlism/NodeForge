"""Service model and lifecycle states for NodeForge."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ServiceStatus(str, Enum):
    """Lifecycle states of a service."""

    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"
    FAILED = "failed"


@dataclass
class Service:
    """Definition and runtime state of a NodeForge service."""

    service_id: str
    command: str
    status: ServiceStatus = ServiceStatus.STOPPED
    pid: Optional[int] = None
    restart_attempts: int = 0
    max_restart_attempts: int = 3

    def mark_starting(self) -> None:
        """Mark the service as starting."""
        self.status = ServiceStatus.STARTING

    def mark_running(self, pid: int) -> None:
        """Mark the service as running."""
        self.status = ServiceStatus.RUNNING
        self.pid = pid

    def mark_stopping(self) -> None:
        """Mark the service as stopping."""
        self.status = ServiceStatus.STOPPING

    def mark_stopped(self) -> None:
        """Mark the service as stopped."""
        self.status = ServiceStatus.STOPPED
        self.pid = None

    def mark_crashed(self) -> None:
        """Mark the service as crashed."""
        self.status = ServiceStatus.CRASHED

    def mark_failed(self) -> None:
        """Mark the service as failed."""
        self.status = ServiceStatus.FAILED
        self.pid = None