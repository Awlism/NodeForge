"""Service manager for NodeForge."""

import asyncio
from typing import Dict, Optional

from freemesh.service import Service


class ServiceManager:
    """Manage NodeForge services and their running processes."""

    def __init__(self, max_restart_attempts: int = 3):
        self.max_restart_attempts = max_restart_attempts
        self._services: Dict[str, asyncio.subprocess.Process] = {}
        self._service_models: Dict[str, Service] = {}

    def get_service(self, service_id: str) -> Optional[Service]:
        """Return a service model by ID."""
        return self._service_models.get(service_id)

    def get_process(
        self,
        service_id: str,
    ) -> Optional[asyncio.subprocess.Process]:
        """Return the running process for a service."""
        return self._services.get(service_id)

    def has_service(self, service_id: str) -> bool:
        """Return whether a service exists."""
        return service_id in self._service_models

    def list_services(self) -> list[Service]:
        """Return all registered services."""
        return list(self._service_models.values())

    def cleanup_exited_service(
        self,
        service_id: str,
    ) -> bool:
        """Remove a service whose process has already exited.

        Running services are never removed by this method.
        Returns True when an exited service was removed.
        """

        process = self._services.get(service_id)

        if process is None or process.returncode is None:
            return False

        self._services.pop(service_id, None)
        self._service_models.pop(service_id, None)

        return True

    async def start_service(
        self,
        service_id: str,
        command: str,
    ) -> Service:
        """Start a new service process."""

        if not service_id:
            raise ValueError("service_id is required")

        if not command:
            raise ValueError("command is required")

        existing_process = self._services.get(service_id)

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
            max_restart_attempts=self.max_restart_attempts,
        )

        self._service_models[service_id] = service
        service.mark_starting()

        try:
            process = await asyncio.create_subprocess_shell(command)

            self._services[service_id] = process
            service.mark_running(pid=process.pid)

            return service

        except Exception:
            service.mark_failed()
            raise

    async def stop_service(
        self,
        service_id: str,
    ) -> Service:
        """Stop a running service."""

        service = self._service_models.get(service_id)
        process = self._services.get(service_id)

        if service is None or process is None:
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
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()

            service.mark_stopped()

        except Exception:
            service.mark_failed()
            raise

        finally:
            self._services.pop(service_id, None)
            self._service_models.pop(service_id, None)

        return service

    async def stop_all(self) -> None:
        """Stop all managed services."""

        for service_id in list(self._service_models):
            try:
                await self.stop_service(service_id)
            except Exception:
                pass