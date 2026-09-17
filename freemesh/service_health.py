"""Runtime health checks for NodeForge services."""

from __future__ import annotations

import os
from typing import Optional

from freemesh.service import (
    Service,
    ServiceHealth,
    ServiceStatus,
)


class ServiceHealthChecker:
    """Check whether a NodeForge service is actually healthy."""

    def __init__(self, allow_zombie_process: bool = False) -> None:
        self.allow_zombie_process = allow_zombie_process

    def check(self, service: Service) -> ServiceHealth:
        """Check the runtime health of a service."""

        if service.status != ServiceStatus.RUNNING:
            if service.status == ServiceStatus.CRASHED:
                service.health = ServiceHealth.CRASHED
            elif service.status == ServiceStatus.FAILED:
                service.health = ServiceHealth.UNHEALTHY
            else:
                service.health = ServiceHealth.UNKNOWN

            return service.health

        if service.pid is None:
            service.health = ServiceHealth.UNHEALTHY
            return service.health

        if not self.process_exists(service.pid):
            service.mark_crashed()
            return service.health

        if (
            not self.allow_zombie_process
            and self.is_zombie(service.pid)
        ):
            service.mark_crashed()
            return service.health

        service.mark_healthy()
        return service.health

    @staticmethod
    def process_exists(pid: int) -> bool:
        """Return whether a process with the given PID exists."""

        if pid <= 0:
            return False

        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False

        return True

    @staticmethod
    def is_zombie(pid: int) -> bool:
        """Return whether a Linux process is currently a zombie."""

        if pid <= 0:
            return False

        stat_path = f"/proc/{pid}/stat"

        try:
            with open(
                stat_path,
                "r",
                encoding="utf-8",
            ) as file:
                content = file.read()

            closing_parenthesis = content.rfind(")")

            if closing_parenthesis == -1:
                return False

            remaining = content[
                closing_parenthesis + 2:
            ]

            fields = remaining.split()

            if not fields:
                return False

            process_state = fields[0]

            return process_state == "Z"

        except FileNotFoundError:
            return False
        except OSError:
            return False

    def check_pid(
        self,
        pid: Optional[int],
    ) -> bool:
        """Return whether a PID represents a live process."""

        if pid is None:
            return False

        if not self.process_exists(pid):
            return False

        if (
            not self.allow_zombie_process
            and self.is_zombie(pid)
        ):
            return False

        return True