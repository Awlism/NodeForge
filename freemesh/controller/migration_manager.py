"""Real service migration orchestration for NodeForge."""

from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from freemesh.controller.resource_accounting import (
    ResourceAccounting,
)
from freemesh.controller.resource_failover import (
    MigrationPlan,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


@dataclass(frozen=True)
class MigrationResult:
    """Final result of a service migration."""

    service_id: str
    source_node_id: str
    target_node_id: str
    status: str
    pid: Optional[int] = None
    error: Optional[str] = None


class MigrationManager:
    """Coordinate the complete migration lifecycle."""

    def __init__(
        self,
        accounting: Optional[ResourceAccounting] = None,
    ) -> None:
        self.accounting = (
            accounting
            or ResourceAccounting()
        )

        self._active_migrations: set[str] = set()

    def is_migrating(
        self,
        service_id: str,
    ) -> bool:
        return service_id in self._active_migrations

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

    async def _rollback_target(
        self,
        stop_service: Optional[
            Callable[..., Awaitable]
        ],
        node_id: str,
        service_id: str,
    ) -> None:
        """Stop a target service created by a failed migration."""

        if stop_service is None:
            return

        try:
            await stop_service(
                node_id=node_id,
                service_id=service_id,
            )
        except Exception:
            # Rollback must never hide the original
            # migration failure.
            pass

    async def execute(
        self,
        plan: MigrationPlan,
        start_service: Callable[..., Awaitable],
        verify_service: Optional[
            Callable[..., Awaitable]
        ] = None,
        stop_service: Optional[
            Callable[..., Awaitable]
        ] = None,
    ) -> MigrationResult:
        """Execute START → VERIFY → COMMIT migration."""

        if not isinstance(
            plan,
            MigrationPlan,
        ):
            raise TypeError(
                "plan must be a MigrationPlan instance"
            )

        if self.is_migrating(
            plan.service_id
        ):
            return MigrationResult(
                service_id=plan.service_id,
                source_node_id=plan.source_node_id,
                target_node_id=plan.target_node_id,
                status="already_migrating",
                error=(
                    "Service migration already in progress"
                ),
            )

        previous_reservation = (
            self.accounting.get(
                plan.service_id
            )
        )

        self._active_migrations.add(
            plan.service_id
        )

        target_started = False
        target_pid: Optional[int] = None

        try:
            response = await start_service(
                node_id=plan.target_node_id,
                service_id=plan.service_id,
                command=plan.command,
                requirements=plan.requirements,
            )

            if response.payload.get(
                "status"
            ) != "started":
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

            target_started = True

            target_pid = response.payload.get(
                "pid"
            )

            if verify_service is not None:
                verified = await verify_service(
                    node_id=plan.target_node_id,
                    service_id=plan.service_id,
                )

                if not verified:
                    await self._rollback_target(
                        stop_service=stop_service,
                        node_id=plan.target_node_id,
                        service_id=plan.service_id,
                    )

                    return MigrationResult(
                        service_id=plan.service_id,
                        source_node_id=plan.source_node_id,
                        target_node_id=plan.target_node_id,
                        status="verification_failed",
                        pid=target_pid,
                        error=(
                            "Target service failed health verification"
                        ),
                    )

            if previous_reservation is None:
                self.reserve_service(
                    service_id=plan.service_id,
                    node_id=plan.target_node_id,
                    requirements=plan.requirements,
                )
            else:
                self.migrate_reservation(
                    service_id=plan.service_id,
                    target_node_id=plan.target_node_id,
                )

            return MigrationResult(
                service_id=plan.service_id,
                source_node_id=plan.source_node_id,
                target_node_id=plan.target_node_id,
                status="migrated",
                pid=target_pid,
            )

        except Exception as exc:
            if target_started:
                await self._rollback_target(
                    stop_service=stop_service,
                    node_id=plan.target_node_id,
                    service_id=plan.service_id,
                )

            return MigrationResult(
                service_id=plan.service_id,
                source_node_id=plan.source_node_id,
                target_node_id=plan.target_node_id,
                status="failed",
                error=str(exc),
            )

        finally:
            self._active_migrations.discard(
                plan.service_id
            )