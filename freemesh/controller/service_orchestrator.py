cat > freemesh/controller/service_orchestrator.py <<'PY'
"""Service lifecycle orchestration for NodeForge.

This layer coordinates service lifecycle operations without owning
execution, placement, migration, or reconciliation implementations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from freemesh.controller.reconciler import ReconciliationResult
from freemesh.service_requirements import ServiceRequirements

if TYPE_CHECKING:
    from freemesh.controller.controller import Controller


class ServiceOrchestrator:
    """Coordinate service lifecycle operations through the Controller."""

    def __init__(self, controller: "Controller") -> None:
        if controller is None:
            raise TypeError("controller is required")

        self.controller = controller

    async def ensure_running(
        self,
        service_id: str,
        command: str,
        requirements: ServiceRequirements | None = None,
    ):
        """Ensure a service is running using automatic placement."""

        if requirements is None:
            return await self.controller.start_service_auto(
                service_id=service_id,
                command=command,
            )

        return await self.controller.start_service_auto(
            service_id=service_id,
            command=command,
            required_cpu_cores=requirements.cpu_cores,
            required_memory_mb=requirements.memory_mb,
            required_disk_gb=requirements.disk_gb,
        )

    async def ensure_stopped(self, service_id: str):
        """Ensure a registered service is stopped."""

        service = self.controller.service_registry.get_service(
            service_id
        )

        if service is None:
            return None

        return await self.controller.stop_service(
            node_id=service.node_id,
            service_id=service_id,
        )

    async def recover(self, service_id: str):
        """Recover a registered service through migration."""

        service = self.controller.service_registry.get_service(
            service_id
        )

        if service is None:
            return None

        return await self.controller.migrate_service(
            service_id=service_id,
            failed_node_id=service.node_id,
        )

    async def migrate(
        self,
        service_id: str,
        failed_node_id: str,
        timeout_seconds: float = 10.0,
    ):
        """Delegate migration without duplicating migration logic."""

        return await self.controller.migrate_service(
            service_id=service_id,
            failed_node_id=failed_node_id,
            timeout_seconds=timeout_seconds,
        )

    async def reconcile(
        self,
        service_id: str,
    ) -> ReconciliationResult:
        """Reconcile one service against persisted desired state."""

        actual_service = (
            self.controller.service_registry.get_service(
                service_id
            )
        )

        async def start_service(
            service_id: str,
            command: str,
            requirements: ServiceRequirements,
        ):
            return await self.ensure_running(
                service_id=service_id,
                command=command,
                requirements=requirements,
            )

        async def stop_service(service_id: str):
            return await self.ensure_stopped(service_id)

        async def migrate_service(service_id: str):
            return await self.recover(service_id)

        return await self.controller.reconciler.reconcile(
            service_id=service_id,
            actual_service=actual_service,
            start_service=start_service,
            stop_service=stop_service,
            migrate_service=migrate_service,
        )

    async def reconcile_all(
        self,
    ) -> list[ReconciliationResult]:
        """Reconcile every persisted service intent."""

        results: list[ReconciliationResult] = []

        for intent in (
            self.controller.service_intent_registry.list_all()
        ):
            try:
                result = await self.reconcile(
                    intent.service_id
                )
                results.append(result)

            except Exception as exc:
                results.append(
                    ReconciliationResult(
                        service_id=intent.service_id,
                        action="error",
                        changed=False,
                        reason=str(exc),
                    )
                )

        return results
PY