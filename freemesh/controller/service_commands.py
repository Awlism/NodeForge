"""High-level service command layer for NodeForge."""

from __future__ import annotations

from typing import TYPE_CHECKING

from freemesh.service_requirements import ServiceRequirements

if TYPE_CHECKING:
    from freemesh.controller.controller import Controller


class ServiceCommandLayer:
    """Expose high-level service commands without owning service logic."""

    def __init__(
        self,
        controller: "Controller",
    ) -> None:
        if controller is None:
            raise TypeError("controller is required")

        self.controller = controller

    async def start(
        self,
        service_id: str,
        command: str,
        requirements: ServiceRequirements | None = None,
    ):
        """Start a service through the lifecycle manager."""

        return await self.controller.service_lifecycle.start(
            service_id=service_id,
            command=command,
            requirements=requirements,
        )

    async def stop(
        self,
        service_id: str,
    ):
        """Stop a service through the lifecycle manager."""

        return await self.controller.service_lifecycle.stop(
            service_id=service_id,
        )

    async def restart(
        self,
        service_id: str,
        command: str,
        requirements: ServiceRequirements | None = None,
    ):
        """Restart a service through the lifecycle manager."""

        return await self.controller.service_lifecycle.restart(
            service_id=service_id,
            command=command,
            requirements=requirements,
        )

    async def recover(
        self,
        service_id: str,
    ):
        """Recover a service through the recovery coordinator."""

        return await self.controller.service_recovery.recover(
            service_id=service_id,
        )

    async def migrate(
        self,
        service_id: str,
        failed_node_id: str,
        timeout_seconds: float = 10.0,
    ):
        """Migrate a service through the existing orchestrator."""

        return await self.controller.service_orchestrator.migrate(
            service_id=service_id,
            failed_node_id=failed_node_id,
            timeout_seconds=timeout_seconds,
        )

    async def status(
        self,
        service_id: str,
    ):
        """Return the current service status through the Controller."""

        service = (
            self.controller.service_registry.get_service(
                service_id
            )
        )

        if service is None:
            return None

        return await self.controller.status_service(
            node_id=service.node_id,
            service_id=service_id,
        )

    async def reconcile(
        self,
        service_id: str,
    ):
        """Reconcile one service through the orchestrator."""

        return await self.controller.service_orchestrator.reconcile(
            service_id=service_id,
        )

    async def reconcile_all(
        self,
    ):
        """Reconcile all persisted service intents."""

        return await self.controller.service_orchestrator.reconcile_all()