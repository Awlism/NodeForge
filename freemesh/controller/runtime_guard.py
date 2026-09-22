"""Runtime guards for NodeForge service operations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeGuardResult:
    """Result of a runtime guard check."""

    allowed: bool
    reason: str


class RuntimeGuard:
    """Validate basic lifecycle operation preconditions."""

    def validate_service_id(
        self,
        service_id: str,
    ) -> RuntimeGuardResult:
        if not isinstance(service_id, str):
            return RuntimeGuardResult(
                allowed=False,
                reason="service_id_must_be_string",
            )

        if not service_id.strip():
            return RuntimeGuardResult(
                allowed=False,
                reason="service_id_required",
            )

        return RuntimeGuardResult(
            allowed=True,
            reason="valid",
        )

    def validate_command(
        self,
        command: str,
    ) -> RuntimeGuardResult:
        if not isinstance(command, str):
            return RuntimeGuardResult(
                allowed=False,
                reason="command_must_be_string",
            )

        if not command.strip():
            return RuntimeGuardResult(
                allowed=False,
                reason="command_required",
            )

        return RuntimeGuardResult(
            allowed=True,
            reason="valid",
        )

    def validate_timeout(
        self,
        timeout_seconds: float,
    ) -> RuntimeGuardResult:
        if timeout_seconds <= 0:
            return RuntimeGuardResult(
                allowed=False,
                reason="timeout_must_be_positive",
            )

        return RuntimeGuardResult(
            allowed=True,
            reason="valid",
        )