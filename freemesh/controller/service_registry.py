"""Service registry for the NodeForge controller."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from freemesh.controller.service_metadata_store import (
    ServiceMetadataStore,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


_UNSET = object()


@dataclass
class ServiceInfo:
    """Information about a service managed by NodeForge."""

    service_id: str
    node_id: str
    status: str
    pid: Optional[int] = None
    command: Optional[str] = None
    requirements: ServiceRequirements = field(
        default_factory=ServiceRequirements
    )
    restart_attempts: int = 0
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(
            timezone.utc
        )
    )


class ServiceRegistry:
    """Registry for services running on NodeForge nodes."""

    def __init__(
        self,
        metadata_store: Optional[
            ServiceMetadataStore
        ] = None,
    ) -> None:
        self._services: Dict[
            str,
            ServiceInfo,
        ] = {}

        self._metadata_store = metadata_store

        if self._metadata_store is not None:
            self.load_from_store()

    def register_service(
        self,
        service_id: str,
        node_id: str,
        status: str,
        pid: Optional[int] = None,
        command: Optional[str] = None,
        requirements: Optional[
            ServiceRequirements
        ] = None,
        restart_attempts: int = 0,
    ) -> ServiceInfo:
        """Register a service or replace its runtime metadata."""

        if not isinstance(
            service_id,
            str,
        ) or not service_id.strip():
            raise ValueError(
                "service_id is required"
            )

        if not isinstance(
            node_id,
            str,
        ) or not node_id.strip():
            raise ValueError(
                "node_id is required"
            )

        if not isinstance(
            status,
            str,
        ) or not status.strip():
            raise ValueError(
                "status is required"
            )

        if requirements is None:
            requirements = ServiceRequirements()

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a "
                "ServiceRequirements instance"
            )

        if (
            not isinstance(
                restart_attempts,
                int,
            )
            or restart_attempts < 0
        ):
            raise ValueError(
                "restart_attempts must be a "
                "non-negative integer"
            )

        service = ServiceInfo(
            service_id=service_id,
            node_id=node_id,
            status=status,
            pid=pid,
            command=command,
            requirements=requirements,
            restart_attempts=restart_attempts,
        )

        self._services[
            service_id
        ] = service

        self._persist(service)

        return service

    def get_service(
        self,
        service_id: str,
    ) -> Optional[ServiceInfo]:
        """Return a service by ID."""

        return self._services.get(
            service_id
        )

    def list_services(
        self,
    ) -> List[ServiceInfo]:
        """Return all registered services."""

        return list(
            self._services.values()
        )

    def list_node_services(
        self,
        node_id: str,
    ) -> List[ServiceInfo]:
        """Return all services assigned to a node."""

        return [
            service
            for service in self._services.values()
            if service.node_id == node_id
        ]

    def update_service(
        self,
        service_id: str,
        status: object = _UNSET,
        pid: object = _UNSET,
        node_id: object = _UNSET,
        command: object = _UNSET,
        requirements: object = _UNSET,
        restart_attempts: object = _UNSET,
    ) -> ServiceInfo:
        """Update runtime state or placement metadata."""

        service = self._services.get(
            service_id
        )

        if service is None:
            raise KeyError(
                f"Service {service_id} not found"
            )

        if status is not _UNSET:
            if (
                not isinstance(
                    status,
                    str,
                )
                or not status.strip()
            ):
                raise ValueError(
                    "status must be a non-empty string"
                )

            service.status = status

        if pid is not _UNSET:
            if (
                pid is not None
                and (
                    not isinstance(
                        pid,
                        int,
                    )
                    or pid <= 0
                )
            ):
                raise ValueError(
                    "pid must be a positive integer or None"
                )

            service.pid = pid

        if node_id is not _UNSET:
            if (
                not isinstance(
                    node_id,
                    str,
                )
                or not node_id.strip()
            ):
                raise ValueError(
                    "node_id must be a non-empty string"
                )

            service.node_id = node_id

        if command is not _UNSET:
            if (
                command is not None
                and not isinstance(
                    command,
                    str,
                )
            ):
                raise TypeError(
                    "command must be a string or None"
                )

            service.command = command

        if requirements is not _UNSET:
            if (
                requirements is not None
                and not isinstance(
                    requirements,
                    ServiceRequirements,
                )
            ):
                raise TypeError(
                    "requirements must be a "
                    "ServiceRequirements instance "
                    "or None"
                )

            if requirements is not None:
                service.requirements = requirements

        if restart_attempts is not _UNSET:
            if (
                not isinstance(
                    restart_attempts,
                    int,
                )
                or restart_attempts < 0
            ):
                raise ValueError(
                    "restart_attempts must be "
                    "a non-negative integer"
                )

            service.restart_attempts = (
                restart_attempts
            )

        service.updated_at = datetime.now(
            timezone.utc
        )

        self._persist(service)

        return service

    def move_service(
        self,
        service_id: str,
        node_id: str,
        pid: Optional[int] = None,
        status: str = "running",
    ) -> ServiceInfo:
        """Move an existing service to another node."""

        if not node_id:
            raise ValueError(
                "node_id is required"
            )

        return self.update_service(
            service_id=service_id,
            status=status,
            pid=pid,
            node_id=node_id,
        )

    def remove_service(
        self,
        service_id: str,
    ) -> Optional[ServiceInfo]:
        """Remove a service from the registry."""

        service = self._services.pop(
            service_id,
            None,
        )

        if (
            service is not None
            and self._metadata_store is not None
        ):
            self._metadata_store.delete(
                service_id
            )

        return service

    def remove_node_services(
        self,
        node_id: str,
    ) -> List[ServiceInfo]:
        """Remove all services assigned to a node."""

        removed: List[ServiceInfo] = []

        for (
            service_id,
            service,
        ) in list(
            self._services.items()
        ):
            if service.node_id == node_id:
                removed.append(
                    self._services.pop(
                        service_id
                    )
                )

                if self._metadata_store is not None:
                    self._metadata_store.delete(
                        service_id
                    )

        return removed

    def service_count(
        self,
    ) -> int:
        """Return the number of registered services."""

        return len(
            self._services
        )

    def clear(
        self,
    ) -> None:
        """Clear all registered services."""

        service_ids = list(
            self._services
        )

        self._services.clear()

        if self._metadata_store is not None:
            for service_id in service_ids:
                self._metadata_store.delete(
                    service_id
                )

    def load_from_store(
        self,
    ) -> None:
        """Load persisted services into the registry."""

        if self._metadata_store is None:
            return

        self._services.clear()

        for metadata in (
            self._metadata_store.list_all()
        ):
            service = ServiceInfo(
                service_id=metadata[
                    "service_id"
                ],
                node_id=metadata[
                    "node_id"
                ],
                status=metadata[
                    "status"
                ],
                pid=metadata[
                    "pid"
                ],
                command=metadata[
                    "command"
                ],
                requirements=metadata[
                    "requirements"
                ],
                restart_attempts=metadata.get(
                    "restart_attempts",
                    0,
                ),
                updated_at=metadata[
                    "updated_at"
                ],
            )

            self._services[
                service.service_id
            ] = service

    def _persist(
        self,
        service: ServiceInfo,
    ) -> None:
        """Persist service metadata."""

        if self._metadata_store is None:
            return

        self._metadata_store.save(
            service_id=service.service_id,
            node_id=service.node_id,
            pid=service.pid,
            status=service.status,
            health="unknown",
            command=service.command,
            requirements=service.requirements,
            restart_attempts=(
                service.restart_attempts
            ),
            updated_at=service.updated_at,
        )