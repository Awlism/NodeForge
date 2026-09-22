"""Service manager for NodeForge."""

from __future__ import annotations

import asyncio
import shlex
from typing import Dict, Optional

from freemesh.service import Service


class ServiceManager:
    """Manage NodeForge services and their running processes."""

    DEFAULT_STOP_TIMEOUT_SECONDS = 5.0
    MAX_SERVICE_ID_LENGTH = 128
    MAX_COMMAND_LENGTH = 4096

    def __init__(
        self,
        max_restart_attempts: int = 3,
        stop_timeout_seconds: float = (
            DEFAULT_STOP_TIMEOUT_SECONDS
        ),
    ):
        if max_restart_attempts < 0:
            raise ValueError(
                "max_restart_attempts cannot be negative"
            )

        if stop_timeout_seconds <= 0:
            raise ValueError(
                "stop_timeout_seconds must be positive"
            )

        self.max_restart_attempts = max_restart_attempts
        self.stop_timeout_seconds = (
            stop_timeout_seconds
        )

        self._services: Dict[
            str,
            asyncio.subprocess.Process,
        ] = {}

        self._service_models: Dict[
            str,
            Service,
        ] = {}

    @classmethod
    def validate_service_id(
        cls,
        service_id: str,
    ) -> None:
        if not isinstance(service_id, str):
            raise TypeError(
                "service_id must be a string"
            )

        service_id = service_id.strip()

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        if len(service_id) > cls.MAX_SERVICE_ID_LENGTH:
            raise ValueError(
                "service_id is too long"
            )

    @classmethod
    def validate_command(
        cls,
        command: str,
    ) -> None:
        if not isinstance(command, str):
            raise TypeError(
                "command must be a string"
            )

        if not command.strip():
            raise ValueError(
                "command is required"
            )

        if len(command) > cls.MAX_COMMAND_LENGTH:
            raise ValueError(
                "command is too long"
            )

        try:
            shlex.split(command)
        except ValueError as exc:
            raise ValueError(
                "command contains invalid shell quoting"
            ) from exc

    def get_service(
        self,
        service_id: str,
    ) -> Optional[Service]:
        """Return a service model by ID."""

        return self._service_models.get(
            service_id
        )

    def get_process(
        self,
        service_id: str,
    ) -> Optional[asyncio.subprocess.Process]:
        """Return the running process for a service."""

        return self._services.get(
            service_id
        )

    def has_service(
        self,
        service_id: str,
    ) -> bool:
        """Return whether a service exists."""

        return service_id in self._service_models

    def list_services(
        self,
    ) -> list[Service]:
        """Return all registered services."""

        return list(
            self._service_models.values()
        )

    def cleanup_exited_service(
        self,
        service_id: str,
    ) -> bool:
        """Remove a service whose process has exited."""

        process = self._services.get(
            service_id
        )

        if (
            process is None
            or process.returncode is None
        ):
            return False

        self._services.pop(
            service_id,
            None,
        )

        self._service_models.pop(
            service_id,
            None,
        )

        return True

    async def _spawn_process(
        self,
        command: str,
    ) -> asyncio.subprocess.Process:
        """Create a service process without invoking a shell."""

        self.validate_command(command)

        argv = shlex.split(command)

        if not argv:
            raise ValueError(
                "command produced no executable"
            )

        return await asyncio.create_subprocess_exec(
            *argv,
        )

    async def start_service(
        self,
        service_id: str,
        command: str,
    ) -> Service:
        """Start a new service process."""

        self.validate_service_id(
            service_id
        )

        self.validate_command(
            command
        )

        existing_process = self._services.get(
            service_id
        )

        if (
            existing_process is not None
            and existing_process.returncode is None
        ):
            raise RuntimeError(
                f"Service {service_id} is already running"
            )

        service = Service(
            service_id=service_id,
            command=command,
            max_restart_attempts=(
                self.max_restart_attempts
            ),
        )

        self._service_models[
            service_id
        ] = service

        service.mark_starting()

        try:
            process = await self._spawn_process(
                command
            )

            self._services[
                service_id
            ] = process

            service.mark_running(
                pid=process.pid
            )

            return service

        except asyncio.CancelledError:
            self._service_models.pop(
                service_id,
                None,
            )
            self._services.pop(
                service_id,
                None,
            )
            raise

        except Exception:
            self._service_models.pop(
                service_id,
                None,
            )
            self._services.pop(
                service_id,
                None,
            )

            service.mark_failed()
            raise

    async def stop_service(
        self,
        service_id: str,
    ) -> Service:
        """Stop a running service."""

        self.validate_service_id(
            service_id
        )

        service = self._service_models.get(
            service_id
        )

        process = self._services.get(
            service_id
        )

        if (
            service is None
            or process is None
        ):
            raise KeyError(
                f"Service {service_id} not found"
            )

        service.mark_stopping()

        try:
            if process.returncode is None:
                process.terminate()

                try:
                    await asyncio.wait_for(
                        process.wait(),
                        timeout=(
                            self.stop_timeout_seconds
                        ),
                    )

                except asyncio.TimeoutError:
                    process.kill()

                    await asyncio.wait_for(
                        process.wait(),
                        timeout=(
                            self.stop_timeout_seconds
                        ),
                    )

            service.mark_stopped()

        except asyncio.CancelledError:
            if process.returncode is None:
                try:
                    process.kill()
                    await process.wait()
                except Exception:
                    pass

            service.mark_failed()
            raise

        except Exception:
            service.mark_failed()
            raise

        finally:
            self._services.pop(
                service_id,
                None,
            )

            self._service_models.pop(
                service_id,
                None,
            )

        return service

    async def stop_all(self) -> None:
        """Stop all managed services."""

        service_ids = list(
            self._service_models
        )

        for service_id in service_ids:
            try:
                await self.stop_service(
                    service_id
                )
            except (
                asyncio.CancelledError,
            ):
                raise
            except Exception:
                continue