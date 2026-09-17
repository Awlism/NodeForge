"""Desired service intent model for NodeForge."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from freemesh.service_requirements import ServiceRequirements


class DesiredState(str, Enum):
    """Desired lifecycle states for a NodeForge service."""

    RUNNING = "running"
    STOPPED = "stopped"


@dataclass
class ServiceIntent:
    """Describe the desired state of a service."""

    service_id: str
    desired_state: DesiredState
    command: Optional[str] = None
    requirements: ServiceRequirements = field(
        default_factory=ServiceRequirements
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not self.service_id:
            raise ValueError(
                "service_id is required"
            )

        if not isinstance(
            self.desired_state,
            DesiredState,
        ):
            raise TypeError(
                "desired_state must be a DesiredState"
            )

        if self.command is not None and not self.command:
            raise ValueError(
                "command cannot be empty"
            )

        if not isinstance(
            self.requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

    def set_desired_state(
        self,
        desired_state: DesiredState,
    ) -> None:
        """Change the desired state."""

        if not isinstance(
            desired_state,
            DesiredState,
        ):
            raise TypeError(
                "desired_state must be a DesiredState"
            )

        self.desired_state = desired_state
        self.updated_at = datetime.now(
            timezone.utc
        )

    def update(
        self,
        desired_state: Optional[DesiredState] = None,
        command: Optional[str] = None,
        requirements: Optional[
            ServiceRequirements
        ] = None,
    ) -> None:
        """Update the desired service configuration."""

        if desired_state is not None:
            if not isinstance(
                desired_state,
                DesiredState,
            ):
                raise TypeError(
                    "desired_state must be a DesiredState"
                )

            self.desired_state = desired_state

        if command is not None:
            if not command:
                raise ValueError(
                    "command cannot be empty"
                )

            self.command = command

        if requirements is not None:
            if not isinstance(
                requirements,
                ServiceRequirements,
            ):
                raise TypeError(
                    "requirements must be a ServiceRequirements instance"
                )

            self.requirements = requirements

        self.updated_at = datetime.now(
            timezone.utc
        )