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
    """Coordinate transactional service migrations.

    Migration is intentionally split into two responsibilities:

    1. Runtime orchestration:
       - start target
       - verify target
       - fence source
    2. Resource transaction:
       - create or move reservation
       - restore reservation on rollback

    The Controller owns the canonical ServiceRegistry state.
    MigrationManager must therefore never directly modify
    ServiceRegistry.
    """

    def __init__(
        self,
        accounting: Optional[ResourceAccounting] = None,
    ) -> None:
        self.accounting = (
            accounting
            if accounting is not None
            else ResourceAccounting()
        )

        self._active_migrations: set[str] = set()

    # =========================================================
    # MIGRATION STATE
    # =========================================================

    def is_migrating(
        self,
        service_id: str,
    ) -> bool:
        """Return whether a service is currently migrating."""

        return service_id in self._active_migrations

    # =========================================================
    # RESOURCE RESERVATION HELPERS
    # =========================================================

    def reserve_service(
        self,
        service_id: str,
        node_id: str,
        requirements: ServiceRequirements,
    ) -> None:
        """Reserve resources for a service on a node."""

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a "
                "ServiceRequirements instance"
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
        """Release a service resource reservation."""

        self.accounting.release(
            service_id
        )

    def migrate_reservation(
        self,
        service_id: str,
        target_node_id: str,
    ):
        """Move an existing reservation to another node."""

        return self.accounting.move(
            service_id=service_id,
            target_node_id=target_node_id,
        )

    # =========================================================
    # ROLLBACK HELPERS
    # =========================================================

    async def _rollback_target(
        self,
        stop_service,
        node_id: str,
        service_id: str,
    ) -> None:
        """Stop a target that was started by a failed migration.

        The supplied stop callback is expected to disable registry
        and resource-accounting side effects. MigrationManager owns
        the transaction and therefore rollback must remain isolated
        from canonical service state.
        """

        if stop_service is None:
            return

        try:
            await stop_service(
                node_id=node_id,
                service_id=service_id,
            )
        except Exception:
            # Rollback is best-effort. The original migration
            # failure must still be reported to the caller.
            pass

    def _restore_reservation(
        self,
        service_id: str,
        previous_reservation,
    ) -> None:
        """Restore the reservation that existed before migration.

        If there was no previous reservation, the service must remain
        unreserved after rollback. This is important when migration
        starts from an already-failed/offline node whose reservation
        was intentionally released before planning.
        """

        current = self.accounting.get(
            service_id
        )

        if current is not None:
            self.accounting.release(
                service_id
            )

        if previous_reservation is None:
            return

        self.accounting.reserve(
            service_id=service_id,
            node_id=previous_reservation.node_id,
            cpu_cores=previous_reservation.cpu_cores,
            memory_mb=previous_reservation.memory_mb,
            disk_gb=previous_reservation.disk_gb,
        )

    # =========================================================
    # MIGRATION EXECUTION
    # =========================================================

    async def execute(
        self,
        plan: MigrationPlan,
        start_service,
        verify_service=None,
        stop_service=None,
        pre_commit=None,
    ) -> MigrationResult:
        """Execute a transactional service migration.

        Transaction order:

        1. Capture the original resource reservation.
        2. Start the target service.
        3. Verify the target service.
        4. Commit the resource reservation move/create.
        5. Fence the source service.
        6. Return a successful migration result.

        If any step fails:

        - target is stopped when necessary;
        - resource accounting is restored;
        - no ServiceRegistry mutation is performed here.

        The Controller performs the final canonical registry commit
        only after this method returns ``status="migrated"``.
        """

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
                    "Service migration already "
                    "in progress"
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
        reservation_changed = False
        target_pid: Optional[int] = None

        try:
            # =================================================
            # STEP 1: START TARGET
            # =================================================

            response = await start_service(
                node_id=plan.target_node_id,
                service_id=plan.service_id,
                command=plan.command,
                requirements=plan.requirements,
            )

            payload = response.payload

            if not isinstance(
                payload,
                dict,
            ):
                return MigrationResult(
                    service_id=plan.service_id,
                    source_node_id=plan.source_node_id,
                    target_node_id=plan.target_node_id,
                    status="failed",
                    error=(
                        "Invalid target start "
                        "response"
                    ),
                )

            target_status = payload.get(
                "status"
            )

            if target_status not in {
                "started",
                "running",
            }:
                return MigrationResult(
                    service_id=plan.service_id,
                    source_node_id=plan.source_node_id,
                    target_node_id=plan.target_node_id,
                    status="failed",
                    error=(
                        payload.get(
                            "error"
                        )
                        or (
                            "Target node failed "
                            "to start service"
                        )
                    ),
                )

            target_started = True

            target_pid = payload.get(
                "pid"
            )

            # =================================================
            # STEP 2: VERIFY TARGET
            # =================================================

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
                            "Target service failed "
                            "health verification"
                        ),
                    )

            # =================================================
            # STEP 3: RESOURCE COMMIT
            # =================================================
            #
            # Resource accounting is intentionally changed only
            # after target start + verification succeed.
            #
            # If there was an existing reservation, move it.
            # If there was no reservation, create one.

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

            reservation_changed = True

            # =================================================
            # STEP 4: SOURCE FENCE
            # =================================================
            #
            # This callback is responsible only for ensuring that
            # the old runtime can no longer be considered active.
            #
            # The callback may treat an already-disconnected source
            # as successfully fenced.

            if pre_commit is not None:
                await pre_commit(
                    node_id=plan.source_node_id,
                    service_id=plan.service_id,
                )

            # =================================================
            # STEP 5: SUCCESS
            # =================================================

            return MigrationResult(
                service_id=plan.service_id,
                source_node_id=plan.source_node_id,
                target_node_id=plan.target_node_id,
                status="migrated",
                pid=target_pid,
            )

        except Exception as exc:
            # =================================================
            # ROLLBACK TARGET
            # =================================================

            if target_started:
                await self._rollback_target(
                    stop_service=stop_service,
                    node_id=plan.target_node_id,
                    service_id=plan.service_id,
                )

            # =================================================
            # ROLLBACK RESOURCES
            # =================================================

            if reservation_changed:
                try:
                    self._restore_reservation(
                        service_id=plan.service_id,
                        previous_reservation=(
                            previous_reservation
                        ),
                    )
                except Exception:
                    # Preserve the original migration error.
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