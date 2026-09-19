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

    def _restore_reservation(
        self,
        service_id: str,
        previous_reservation,
    ) -> None:
        """Restore the reservation that existed before migration."""

        current_reservation = (
            self.accounting.get(
                service_id
            )
        )

        if current_reservation is not None:
            self.accounting.release(
                service_id
            )

        if previous_reservation is not None:
            self.accounting.reserve(
                service_id=service_id,
                node_id=previous_reservation.node_id,
                cpu_cores=previous_reservation.cpu_cores,
                memory_mb=previous_reservation.memory_mb,
                disk_gb=previous_reservation.disk_gb,
            )

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
        pre_commit: Optional[
            Callable[..., Awaitable]
        ] = None,
    ) -> MigrationResult:
        """Execute START → VERIFY → FENCE → COMMIT migration."""

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
        reservation_moved = False
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

            # Move resource ownership before fencing the source.
            #
            # If source fencing fails, the exception path restores
            # the previous reservation after the target is rolled
            # back. This keeps resource ownership transactional.
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

            reservation_moved = True

            # Fence the source runtime before the migration is
            # considered committed. The Controller supplies this
            # callback so the source process is actually stopped
            # without prematurely changing controller metadata.
            if pre_commit is not None:
                await pre_commit(
                    node_id=plan.source_node_id,
                    service_id=plan.service_id,
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

            if reservation_moved:
                try:
                    self._restore_reservation(
                        service_id=plan.service_id,
                        previous_reservation=(
                            previous_reservation
                        ),
                    )
                except Exception:
                    # The Controller performs an additional
                    # source-state restoration after execute()
                    # returns a failed result.
                    pass

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