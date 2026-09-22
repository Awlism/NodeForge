"""Observational MVP validation for NodeForge."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MVPCheck:
    """Result of one MVP capability check."""

    name: str
    passed: bool
    detail: str


class MVPValidator:
    """Validate structural MVP capabilities."""

    REQUIRED_ATTRIBUTES = (
        "registry",
        "resource_registry",
        "service_registry",
        "service_intent_registry",
        "reconciler",
        "resource_accounting",
        "resource_scheduler",
        "service_placement",
        "migration_manager",
        "migration_registry",
        "service_orchestrator",
        "service_lifecycle",
        "service_recovery",
        "service_health",
        "service_commands",
    )

    REQUIRED_METHODS = (
        "start_service_auto",
        "start_service",
        "stop_service",
        "status_service",
        "migrate_service",
        "reconcile_service",
        "reconcile_all_services",
    )

    def __init__(
        self,
        controller: Any,
    ) -> None:
        if controller is None:
            raise TypeError(
                "controller is required"
            )

        self.controller = controller

    def run(self) -> list[MVPCheck]:
        """Run non-invasive structural checks."""

        checks: list[MVPCheck] = []

        for name in self.REQUIRED_ATTRIBUTES:
            present = hasattr(
                self.controller,
                name,
            )

            checks.append(
                MVPCheck(
                    name=(
                        "controller.attribute."
                        + name
                    ),
                    passed=present,
                    detail=(
                        "present"
                        if present
                        else "missing"
                    ),
                )
            )

        for name in self.REQUIRED_METHODS:
            present = callable(
                getattr(
                    self.controller,
                    name,
                    None,
                )
            )

            checks.append(
                MVPCheck(
                    name=(
                        "controller.method."
                        + name
                    ),
                    passed=present,
                    detail=(
                        "callable"
                        if present
                        else "missing"
                    ),
                )
            )

        return checks

    def is_ready(self) -> bool:
        """Return True when all checks pass."""

        return all(
            check.passed
            for check in self.run()
        )

    def summary(self) -> dict[str, Any]:
        """Return machine-readable validation data."""

        checks = self.run()

        passed = sum(
            check.passed
            for check in checks
        )

        failed = len(checks) - passed

        return {
            "ready": failed == 0,
            "passed": passed,
            "failed": failed,
            "checks": [
                {
                    "name": check.name,
                    "passed": check.passed,
                    "detail": check.detail,
                }
                for check in checks
            ],
        }