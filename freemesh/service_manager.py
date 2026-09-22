"""Service manager for NodeForge."""

from __future__ import annotations

import asyncio
import shlex
from typing import Dict, Optional

from freemesh.service import Service


class ServiceManager:
    """Manage NodeForge services and their processes."""

    DEFAULT_STOP_TIMEOUT_SECONDS = 5.0
    MAX_SERVICE_ID_LENGTH = 128
    MAX_COMMAND_LENGTH = 4096

    def __init__(
        self,
        max_restart_attempts: int = 3,
        stop_timeout_seconds: float = DEFAULT_STOP_TIMEOUT_SECONDS,
    ) -> None:
        if (
            not isinstance(max_restart_attempts, int)
            or isinstance(max_restart_attempts, bool)
            or max_restart_attempts < 0
        ):
            raise ValueError(
                "max_restart_attempts must be a non-negative integer"
            )

        if (
            not isinstance(stop_timeout_seconds, (int, float))
            or isinstance(stop_timeout_seconds, bool)
            or stop_timeout_seconds <= 0
        ):
            raise ValueError(
                "stop_timeout_seconds must be positive"
            )

        self.max_restart_attempts = max_restart_attempts
        self.stop_timeout_seconds = float(
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

        self._lock = asyncio.Lock()

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
            argv = shlex.split(command)
        except ValueError as exc:
            raise ValueError(
                "command contains invalid quoting"
            ) from exc

        if not argv:
            raise ValueError(
                "command produced no executable"
            )

    @classmethod
    def command_to_argv(
        cls,
        command: str,
    ) -> list[str]:
        cls.validate_command(command)

        return shlex.split(command)

    def get_service(
        self,
        service_id: str,
    ) -> Optional[Service]:
        return self._service_models.get(
            service_id
        )

    def get_process(
        self,
        service_id: str,
    ) -> Optional[asyncio.subprocess.Process]:
        return self._services.get(
            service_id
        )

    def has_service(
        self,
        service_id: str,
    ) -> bool:
        return service_id in self._service_models

    def list_services(
        self,
    ) -> list[Service]:
        return list(
            self._service_models.values()
        )

    def register_process(
        self,
        service_id: str,
        process: asyncio.subprocess.Process,
    ) -> None:
        """Register an externally-created process safely."""

        self.validate_service_id(
            service_id
        )

        if not isinstance(
            process,
            asyncio.subprocess.Process,
        ):
            raise TypeError(
                "process must be an asyncio subprocess Process"
            )

        self._services[
            service_id
        ] = process

    def unregister_process(
        self,
        service_id: str,
    ) -> Optional[
        asyncio.subprocess.Process
    ]:
        return self._services.pop(
            service_id,
            None,
        )

    async def _spawn_process(
        self,
        command: str,
    ) -> asyncio.subprocess.Process:
        argv = self.command_to_argv(
            command
        )

        return await asyncio.create_subprocess_exec(
            *argv,
        )

    async def start_service(
        self,
        service_id: str,
        command: str,
    ) -> Service:
        self.validate_service_id(
            service_id
        )
        self.validate_command(
            command
        )

        async with self._lock:
            existing_process = (
                self._services.get(
                    service_id
                )
            )

            if (
                existing_process is not None
                and existing_process.returncode is None
            ):
                raise RuntimeError(
                    f"Service {service_id} is already running"
                )

            self._services.pop(
                service_id,
                None,
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
        self.validate_service_id(
            service_id
        )

        async with self._lock:
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

    async def stop_all(
        self,
    ) -> None:
        service_ids = list(
            self._service_models
        )

        errors: list[Exception] = []

        for service_id in service_ids:
            try:
                await self.stop_service(
                    service_id
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(
                    exc
                )

        if errors:
            raise RuntimeError(
                f"Failed to stop "
                f"{len(errors)} service(s)"
            )