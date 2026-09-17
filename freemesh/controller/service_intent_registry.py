"""Registry for NodeForge service desired intents."""

from __future__ import annotations

from typing import Optional

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


class ServiceIntentRegistry:
    """Store and manage desired state for services."""

    def __init__(self) -> None:
        self._intents: dict[str, ServiceIntent] = {}

    def register(
        self,
        intent: ServiceIntent,
    ) -> ServiceIntent:
        """Register a new service intent."""

        if not isinstance(intent, ServiceIntent):
            raise TypeError(
                "intent must be a ServiceIntent instance"
            )

        if intent.service_id in self._intents:
            raise ValueError(
                f"Service intent already exists: "
                f"{intent.service_id}"
            )

        self._intents[intent.service_id] = intent

        return intent

    def get(
        self,
        service_id: str,
    ) -> Optional[ServiceIntent]:
        """Return an intent by service ID."""

        return self._intents.get(service_id)

    def require(
        self,
        service_id: str,
    ) -> ServiceIntent:
        """Return an intent or raise KeyError."""

        intent = self.get(service_id)

        if intent is None:
            raise KeyError(
                f"Service intent not found: {service_id}"
            )

        return intent

    def update(
        self,
        service_id: str,
        desired_state: Optional[DesiredState] = None,
        command: Optional[str] = None,
        requirements: Optional[
            ServiceRequirements
        ] = None,
    ) -> ServiceIntent:
        """Update an existing service intent."""

        intent = self.require(service_id)

        intent.update(
            desired_state=desired_state,
            command=command,
            requirements=requirements,
        )

        return intent

    def set_desired_state(
        self,
        service_id: str,
        desired_state: DesiredState,
    ) -> ServiceIntent:
        """Change only the desired state."""

        intent = self.require(service_id)

        intent.set_desired_state(desired_state)

        return intent

    def remove(
        self,
        service_id: str,
    ) -> ServiceIntent:
        """Remove and return a service intent."""

        try:
            return self._intents.pop(service_id)
        except KeyError as exc:
            raise KeyError(
                f"Service intent not found: {service_id}"
            ) from exc

    def exists(
        self,
        service_id: str,
    ) -> bool:
        """Return whether an intent exists."""

        return service_id in self._intents

    def list_all(self) -> list[ServiceIntent]:
        """Return all registered intents."""

        return list(self._intents.values())

    def count(self) -> int:
        """Return the number of registered intents."""

        return len(self._intents)

    def clear(self) -> None:
        """Remove all registered intents."""

        self._intents.clear()