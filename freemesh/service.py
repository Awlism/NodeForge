"""Service model and lifecycle states for NodeForge."""

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


@dataclass
class Service:
    service_id: str
    command: str
    status: ServiceStatus = ServiceStatus.STOPPED
    pid: Optional[int] = None
    restart_attempts: int = 0
    max_restart_attempts: int = 3
    requirements: ServiceRequirements = field(
        default_factory=ServiceRequirements
    )
    node_id: Optional[str] = None

    def mark_starting(self) -> None:
        self.status = ServiceStatus.STARTING

    def mark_running(
        self,
        pid: int,
        node_id: Optional[str] = None,
    ) -> None:
        self.status = ServiceStatus.RUNNING
        self.pid = pid

        if node_id is not None:
            self.node_id = node_id

    def mark_stopping(self) -> None:
        self.status = ServiceStatus.STOPPING

    def mark_stopped(self) -> None:
        self.status = ServiceStatus.STOPPED
        self.pid = None
        self.node_id = None

    def mark_crashed(self) -> None:
        self.status = ServiceStatus.CRASHED

    def mark_failed(self) -> None:
        self.status = ServiceStatus.FAILED
        self.pid = None