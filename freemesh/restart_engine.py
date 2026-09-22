"""Restart engine for NodeForge services."""

from __future__ import annotations

import asyncio

from freemesh.service import (
    Service,
    ServiceHealth,
)
from freemesh.service_health import (
    ServiceHealthChecker,
)
from freemesh.service_manager import (
    ServiceManager,
)


class RestartEngine:
    """Handle automatic service restart attempts."""

    def __init__(
        self,
        max_restart_attempts: int = 3,
        backoff_seconds: float = 0.0,
        health_checker: ServiceHealthChecker | None = None,
        startup_grace_seconds: float = 0.05,
    ) -> None:
        if (
            not isinstance(max_restart_attempts, int)
            or isinstance(max_restart_attempts, bool)
            or max_restart_attempts < 0
        ):
            raise ValueError(
                "max_restart_attempts cannot be negative"
            )

        if (
            not isinstance(backoff_seconds, (int, float))
            or isinstance(backoff_seconds, bool)
            or backoff_seconds < 0
        ):
            raise ValueError(
                "backoff_seconds cannot be negative"
            )

        if (
            not isinstance(startup_grace_seconds, (int, float))
            or isinstance(startup_grace_seconds, bool)
            or startup_grace_seconds < 0
        ):
            raise ValueError(
                "startup_grace_seconds cannot be negative"
            )

        self.max_restart_attempts = (
            max_restart_attempts
        )

        self.backoff_seconds = float(
            backoff_seconds
        )

        self.startup_grace_seconds = float(
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
        argv = ServiceManager.command_to_argv(
            command
        )

        return await asyncio.create_subprocess_exec(
            *argv,
        )

    async def _terminate_process(
        self,
        process: asyncio.subprocess.Process,
        timeout_seconds: float = 2.0,
    ) -> None:
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
        if not isinstance(
            service,
            Service,
        ):
            raise TypeError(
                "service must be a Service instance"
            )

        if not isinstance(
            service_manager,
            ServiceManager,
        ):
            raise TypeError(
                "service_manager must be a ServiceManager instance"
            )

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

                service_manager.register_process(
                    service.service_id,
                    process,
                )

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

                        service_manager.unregister_process(
                            service.service_id
                        )

                        continue

                    except asyncio.TimeoutError:
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

                await self._terminate_process(
                    process
                )

                service_manager.unregister_process(
                    service.service_id
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

                service_manager.unregister_process(
                    service.service_id
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

                service_manager.unregister_process(
                    service.service_id
                )

                service.restart_attempts += 1
                service.mark_failed()

        return False