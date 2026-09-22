"""Restart engine for NodeForge services."""

from __future__ import annotations

import asyncio
import shlex

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

    async def _spawn_process(
        self,
        command: str,
    ) -> asyncio.subprocess.Process:
        """Start a service without invoking a shell."""

        ServiceManager.validate_command(
            command
        )

        argv = shlex.split(command)

        if not argv:
            raise ValueError(
                "command produced no executable"
            )

        return await asyncio.create_subprocess_exec(
            *argv,
        )

    async def _terminate_process(
        self,
        process: asyncio.subprocess.Process,
        timeout_seconds: float = 2.0,
    ) -> None:
        """Terminate a process and force-kill it if necessary."""

        if process.returncode is not None:
            return

        process.terminate()

        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=timeout_seconds,
            )

        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    async def restart(
        self,
        service: Service,
        service_manager: ServiceManager,
        node_id: str | None = None,
    ) -> bool:
        """Restart a crashed service."""

        if service.status.value not in {
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
                delay = (
                    self.backoff_seconds
                    * (
                        2
                        ** (
                            service.restart_attempts - 1
                        )
                    )
                )

                await asyncio.sleep(
                    delay
                )

            process = None

            try:
                process = await self._spawn_process(
                    service.command
                )

                service_manager._services[
                    service.service_id
                ] = process

                service.restart_attempts += 1

                service.mark_running(
                    pid=process.pid,
                    node_id=node_id,
                )

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

                        service.mark_crashed()

                        service_manager._services.pop(
                            service.service_id,
                            None,
                        )

                        continue

                    except asyncio.TimeoutError:
                        pass

                health = (
                    self.health_checker.check(
                        service
                    )
                )

                if health == ServiceHealth.HEALTHY:
                    return True

                await self._terminate_process(
                    process
                )

                service_manager._services.pop(
                    service.service_id,
                    None,
                )

                service.mark_crashed()

            except asyncio.CancelledError:
                if process is not None:
                    try:
                        await self._terminate_process(
                            process
                        )
                    except Exception:
                        pass

                service_manager._services.pop(
                    service.service_id,
                    None,
                )

                raise

            except Exception:
                if process is not None:
                    try:
                        await self._terminate_process(
                            process
                        )
                    except Exception:
                        pass

                service_manager._services.pop(
                    service.service_id,
                    None,
                )

                service.restart_attempts += 1
                service.mark_failed()

        return False