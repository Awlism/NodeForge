"""Service lifecycle coordination for NodeForge."""

from __future__ import annotations

from typing import TYPE_CHECKING

from freemesh.service_requirements import ServiceRequirements

if TYPE_CHECKING:
    from freemesh.controller.service_orchestrator import ServiceOrchestrator


class ServiceLifecycleManager:
    """Coordinate high-level service lifecycle operations."""

    def __init__(
        self,
        orchestrator: "ServiceOrchestrator",
    ) -> None:
        if orchestrator is None:
            raise TypeError("orchestrator is required")

        self.orchestrator = orchestrator

    async def start(
        self,
        service_id: str,
        command: str,
        requirements: ServiceRequirements | None = None,
    ):
        """Start a service through the orchestrator."""

        return await self.orchestrator.ensure_running(
            service_id=service_id,
            command=command,
            requirements=requirements,
        )

    async def stop(
        self,
        service_id: str,
    ):
        """Stop a service through the orchestrator."""

        return await self.orchestrator.ensure_stopped(
            service_id=service_id,
        )

    async def restart(
        self,
        service_id: str,
        command: str,
        requirements: ServiceRequirements | None = None,
    ):
        """Restart a service using the existing lifecycle operations."""

        await self.stop(
            service_id=service_id,
        )

        return await self.start(
            service_id=service_id,
            command=command,
            requirements=requirements,
        )