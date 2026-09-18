"""Restart engine for NodeForge services."""

from __future__ import annotations

import asyncio

from freemesh.service import Service, ServiceHealth
from freemesh.service_health import ServiceHealthChecker
from freemesh.service_manager import ServiceManager


class RestartEngine:
    """Handle automatic service restart attempts."""

    def __init__(
        self,
        max_restart_attempts: int = 3,
        backoff_seconds: float = 0.0,
        health_checker: ServiceHealthChecker | None = None,
        startup_grace_seconds: float = 0.05,
    ) -> None:
        if max_restart_attempts < 0:
            raise ValueError(
                "max_restart_attempts cannot be negative"
            )

        if backoff_seconds < 0:
            raise ValueError(
                "backoff_seconds cannot be negative"
            )

        if startup_grace_seconds < 0:
            raise ValueError(
                "startup_grace_seconds cannot be negative"
            )

        self.max_restart_attempts = (
            max_restart_attempts
        )

        self.backoff_seconds = (
            backoff_seconds
        )

        self.startup_grace_seconds = (
            startup_grace_seconds
        )

        self.health_checker = (
            health_checker
            if health_checker is not None
            else ServiceHealthChecker()
        )

    async def restart(
        self,
        service: Service,
        service_manager: ServiceManager,
        node_id: str | None = None,
    ) -> bool:
        """Restart a crashed service.

        Returns True when the restarted service becomes
        healthy.

        Returns False when all restart attempts are
        exhausted.
        """

        status = service.status

        if hasattr(status, "value"):
            status = status.value

        if status not in {
            "crashed",
            "failed",
        }:
            return False

        max_attempts = min(
            service.max_restart_attempts,
            self.max_restart_attempts,
        )

        while (
            service.restart_attempts
            < max_attempts
        ):
            if (
                self.backoff_seconds > 0
                and service.restart_attempts > 0
            ):
                await asyncio.sleep(
                    self.backoff_seconds
                    * (
                        2
                        ** (
                            service.restart_attempts - 1
                        )
                    )
                )

            process = None

            try:
                process = (
                    await asyncio.create_subprocess_shell(
                        service.command
                    )
                )

                service_manager._services[
                    service.service_id
                ] = process

                service.restart_attempts += 1

                service.mark_running(
                    pid=process.pid,
                    node_id=node_id,
                )

                # Give the process a short startup grace
                # period so very short-lived failures are
                # detected before declaring the restart
                # successful.
                if (
                    self.startup_grace_seconds > 0
                ):
                    try:
                        await asyncio.wait_for(
                            process.wait(),
                            timeout=(
                                self.startup_grace_seconds
                            ),
                        )

                        # The process exited during the
                        # startup grace period.
                        service.mark_crashed()

                        service_manager._services.pop(
                            service.service_id,
                            None,
                        )

                        continue

                    except asyncio.TimeoutError:
                        # The process is still running.
                        pass

                health = (
                    self.health_checker.check(
                        service
                    )
                )

                if (
                    health
                    == ServiceHealth.HEALTHY
                ):
                    return True

                if process.returncode is None:
                    process.terminate()

                    try:
                        await asyncio.wait_for(
                            process.wait(),
                            timeout=2.0,
                        )
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()

                service_manager._services.pop(
                    service.service_id,
                    None,
                )

                service.mark_crashed()

            except Exception:
                service_manager._services.pop(
                    service.service_id,
                    None,
                )

                service.restart_attempts += 1
                service.mark_failed()

        return False