"""Desired-state reconciliation engine for NodeForge."""

from __future__ import annotations

from typing import Optional

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.controller.service_intent_registry import (
    ServiceIntentRegistry,
)


class ReconciliationResult:
    """Result of reconciling one service."""

    def __init__(
        self,
        service_id: str,
        action: str,
        changed: bool,
        reason: str,
    ) -> None:
        self.service_id = service_id
        self.action = action
        self.changed = changed
        self.reason = reason

    def __repr__(self) -> str:
        return (
            "ReconciliationResult("
            f"service_id={self.service_id!r}, "
            f"action={self.action!r}, "
            f"changed={self.changed!r}, "
            f"reason={self.reason!r}"
            ")"
        )


class Reconciler:
    """Compare desired service state with actual state."""

    def __init__(
        self,
        intent_registry: ServiceIntentRegistry,
    ) -> None:
        if not isinstance(
            intent_registry,
            ServiceIntentRegistry,
        ):
            raise TypeError(
                "intent_registry must be a "
                "ServiceIntentRegistry instance"
            )

        self.intent_registry = intent_registry

    async def reconcile(
        self,
        service_id: str,
        actual_service: Optional[object],
        start_service,
        stop_service,
        migrate_service,
    ) -> ReconciliationResult:
        """Reconcile one service against its desired state."""

        intent = self.intent_registry.get(service_id)

        if intent is None:
            return ReconciliationResult(
                service_id=service_id,
                action="none",
                changed=False,
                reason="no_intent",
            )

        if intent.desired_state == DesiredState.RUNNING:
            return await self._reconcile_running(
                intent=intent,
                actual_service=actual_service,
                start_service=start_service,
                migrate_service=migrate_service,
            )

        if intent.desired_state == DesiredState.STOPPED:
            return await self._reconcile_stopped(
                intent=intent,
                actual_service=actual_service,
                stop_service=stop_service,
            )

        return ReconciliationResult(
            service_id=service_id,
            action="none",
            changed=False,
            reason="unsupported_desired_state",
        )

    async def _reconcile_running(
        self,
        intent: ServiceIntent,
        actual_service: Optional[object],
        start_service,
        migrate_service,
    ) -> ReconciliationResult:
        """Ensure a service with RUNNING intent is running."""

        if actual_service is None:
            if not intent.command:
                return ReconciliationResult(
                    service_id=intent.service_id,
                    action="blocked",
                    changed=False,
                    reason="missing_command",
                )

            await start_service(
                intent.service_id,
                intent.command,
                intent.requirements,
            )

            return ReconciliationResult(
                service_id=intent.service_id,
                action="start",
                changed=True,
                reason="service_missing",
            )

        status = getattr(
            actual_service,
            "status",
            None,
        )

        status_value = getattr(
            status,
            "value",
            status,
        )

        if status_value == "running":
            return ReconciliationResult(
                service_id=intent.service_id,
                action="none",
                changed=False,
                reason="already_running",
            )

        if status_value in {
            "stopped",
            "failed",
            "crashed",
        }:
            if not intent.command:
                return ReconciliationResult(
                    service_id=intent.service_id,
                    action="blocked",
                    changed=False,
                    reason="missing_command",
                )

            await start_service(
                intent.service_id,
                intent.command,
                intent.requirements,
            )

            return ReconciliationResult(
                service_id=intent.service_id,
                action="start",
                changed=True,
                reason=f"actual_state_{status_value}",
            )

        if status_value == "migrating":
            return ReconciliationResult(
                service_id=intent.service_id,
                action="none",
                changed=False,
                reason="migration_in_progress",
            )

        return ReconciliationResult(
            service_id=intent.service_id,
            action="migrate",
            changed=True,
            reason="service_requires_reconciliation",
        )

    async def _reconcile_stopped(
        self,
        intent: ServiceIntent,
        actual_service: Optional[object],
        stop_service,
    ) -> ReconciliationResult:
        """Ensure a service with STOPPED intent is stopped."""

        if actual_service is None:
            return ReconciliationResult(
                service_id=intent.service_id,
                action="none",
                changed=False,
                reason="already_absent",
            )

        status = getattr(
            actual_service,
            "status",
            None,
        )

        status_value = getattr(
            status,
            "value",
            status,
        )

        if status_value in {
            "stopped",
            "failed",
        }:
            return ReconciliationResult(
                service_id=intent.service_id,
                action="none",
                changed=False,
                reason=f"already_{status_value}",
            )

        await stop_service(
            intent.service_id
        )

        return ReconciliationResult(
            service_id=intent.service_id,
            action="stop",
            changed=True,
            reason="desired_state_stopped",
        )

    async def reconcile_all(
        self,
        actual_services: dict[str, object],
        start_service,
        stop_service,
        migrate_service,
    ) -> list[ReconciliationResult]:
        """Reconcile all registered service intents."""

        results: list[ReconciliationResult] = []

        for intent in self.intent_registry.list_all():
            actual_service = actual_services.get(
                intent.service_id
            )

            result = await self.reconcile(
                service_id=intent.service_id,
                actual_service=actual_service,
                start_service=start_service,
                stop_service=stop_service,
                migrate_service=migrate_service,
            )

            results.append(result)

        return results