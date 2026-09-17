"""Service migration orchestration for NodeForge."""

from dataclasses import dataclass
from typing import Optional

from freemesh.controller.resource_accounting import (
    ResourceAccounting,
)
from freemesh.controller.resource_failover import (
    MigrationPlan,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


@dataclass
class MigrationResult:
    """Result of a service migration."""

    service_id: str
    source_node_id: str
    target_node_id: str
    status: str
    pid: Optional[int] = None
    error: Optional[str] = None


class MigrationManager:
    """Coordinate service migration and resource reservations."""

    def __init__(
        self,
        accounting: Optional[ResourceAccounting] = None,
    ) -> None:
        self.accounting = (
            accounting
            or ResourceAccounting()
        )

    def reserve_service(
        self,
        service_id: str,
        node_id: str,
        requirements: ServiceRequirements,
    ) -> None:
        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

        self.accounting.reserve(
            service_id=service_id,
            node_id=node_id,
            cpu_cores=requirements.cpu_cores,
            memory_mb=requirements.memory_mb,
            disk_gb=requirements.disk_gb,
        )

    def release_service(
        self,
        service_id: str,
    ) -> None:
        self.accounting.release(
            service_id
        )

    def migrate_reservation(
        self,
        service_id: str,
        target_node_id: str,
    ):
        return self.accounting.move(
            service_id=service_id,
            target_node_id=target_node_id,
        )

    async def execute(
        self,
        plan: MigrationPlan,
        start_service,
    ) -> MigrationResult:
        """Execute a migration plan through a start callback."""

        if not isinstance(
            plan,
            MigrationPlan,
        ):
            raise TypeError(
                "plan must be a MigrationPlan instance"
            )

        try:
            response = await start_service(
                node_id=plan.target_node_id,
                service_id=plan.service_id,
                command=plan.command,
                requirements=plan.requirements,
            )

            status = response.payload.get(
                "status"
            )

            if status != "started":
                return MigrationResult(
                    service_id=plan.service_id,
                    source_node_id=plan.source_node_id,
                    target_node_id=plan.target_node_id,
                    status="failed",
                    error=response.payload.get(
                        "error",
                        "Target node failed to start service",
                    ),
                )

            self.reserve_service(
                service_id=plan.service_id,
                node_id=plan.target_node_id,
                requirements=plan.requirements,
            )

            return MigrationResult(
                service_id=plan.service_id,
                source_node_id=plan.source_node_id,
                target_node_id=plan.target_node_id,
                status="migrated",
                pid=response.payload.get(
                    "pid"
                ),
            )

        except Exception as exc:
            return MigrationResult(
                service_id=plan.service_id,
                source_node_id=plan.source_node_id,
                target_node_id=plan.target_node_id,
                status="failed",
                error=str(exc),
            )