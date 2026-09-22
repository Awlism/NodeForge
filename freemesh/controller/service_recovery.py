"""Service recovery coordination for NodeForge."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from freemesh.controller.service_orchestrator import (
        ServiceOrchestrator,
    )


class ServiceRecoveryCoordinator:
    """Coordinate high-level service recovery operations."""

    def __init__(
        self,
        orchestrator: "ServiceOrchestrator",
    ) -> None:
        if orchestrator is None:
            raise TypeError(
                "orchestrator is required"
            )

        self.orchestrator = orchestrator

    async def recover(
        self,
        service_id: str,
    ):
        """Recover a registered service."""

        return await self.orchestrator.recover(
            service_id=service_id,
        )

    async def recover_from_node(
        self,
        node_id: str,
    ) -> None:
        """Recover all recoverable services from a node."""

        controller = self.orchestrator.controller

        services = (
            controller.service_registry.list_node_services(
                node_id
            )
        )

        for service in services:
            try:
                if service.status in {
                    "stopped",
                    "failed",
                }:
                    continue

                controller.resource_accounting.release(
                    service.service_id
                )

                await self.recover(
                    service.service_id
                )

            except (
                RuntimeError,
                TimeoutError,
                KeyError,
                ValueError,
            ):
                continue

            except Exception:
                continue

    async def recover_failed_service(
        self,
        service_id: str,
        node_id: str,
    ):
        """Recover a service after a runtime failure."""

        controller = self.orchestrator.controller

        service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        if service is None:
            return None

        if service.node_id != node_id:
            return None

        return await self.recover(
            service_id=service_id,
        )