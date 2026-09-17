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