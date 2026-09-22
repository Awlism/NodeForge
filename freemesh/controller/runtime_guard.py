"""Runtime guards for NodeForge service operations."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeGuardResult:
    """Result of a runtime guard check."""

    allowed: bool
    reason: str


class RuntimeGuard:
    """Validate lifecycle operation preconditions and limits."""

    DEFAULT_MAX_SERVICE_ID_LENGTH = 128
    DEFAULT_MAX_COMMAND_LENGTH = 4096
    DEFAULT_MAX_TIMEOUT_SECONDS = 300.0

    def __init__(
        self,
        *,
        max_service_id_length: int = DEFAULT_MAX_SERVICE_ID_LENGTH,
        max_command_length: int = DEFAULT_MAX_COMMAND_LENGTH,
        max_timeout_seconds: float = DEFAULT_MAX_TIMEOUT_SECONDS,
    ) -> None:
        if max_service_id_length <= 0:
            raise ValueError(
                "max_service_id_length must be positive"
            )

        if max_command_length <= 0:
            raise ValueError(
                "max_command_length must be positive"
            )

        if (
            not math.isfinite(max_timeout_seconds)
            or max_timeout_seconds <= 0
        ):
            raise ValueError(
                "max_timeout_seconds must be positive and finite"
            )

        self.max_service_id_length = (
            max_service_id_length
        )
        self.max_command_length = (
            max_command_length
        )
        self.max_timeout_seconds = (
            max_timeout_seconds
        )

    def validate_service_id(
        self,
        service_id: str,
    ) -> RuntimeGuardResult:
        if not isinstance(service_id, str):
            return RuntimeGuardResult(
                allowed=False,
                reason="service_id_must_be_string",
            )

        value = service_id.strip()

        if not value:
            return RuntimeGuardResult(
                allowed=False,
                reason="service_id_required",
            )

        if len(value) > self.max_service_id_length:
            return RuntimeGuardResult(
                allowed=False,
                reason="service_id_too_long",
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

        value = command.strip()

        if not value:
            return RuntimeGuardResult(
                allowed=False,
                reason="command_required",
            )

        if len(value) > self.max_command_length:
            return RuntimeGuardResult(
                allowed=False,
                reason="command_too_long",
            )

        return RuntimeGuardResult(
            allowed=True,
            reason="valid",
        )

    def validate_timeout(
        self,
        timeout_seconds: float,
    ) -> RuntimeGuardResult:
        if not isinstance(
            timeout_seconds,
            (int, float),
        ):
            return RuntimeGuardResult(
                allowed=False,
                reason="timeout_must_be_number",
            )

        if not math.isfinite(timeout_seconds):
            return RuntimeGuardResult(
                allowed=False,
                reason="timeout_must_be_finite",
            )

        if timeout_seconds <= 0:
            return RuntimeGuardResult(
                allowed=False,
                reason="timeout_must_be_positive",
            )

        if timeout_seconds > self.max_timeout_seconds:
            return RuntimeGuardResult(
                allowed=False,
                reason="timeout_too_large",
            )

        return RuntimeGuardResult(
            allowed=True,
            reason="valid",
        )