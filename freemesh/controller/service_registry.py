"""Service registry for the NodeForge controller."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class ServiceInfo:
    """Information about a service managed by NodeForge."""

    service_id: str
    node_id: str
    status: str
    pid: Optional[int] = None
    command: Optional[str] = None
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class ServiceRegistry:
    """In-memory registry for services running on NodeForge nodes."""

    def __init__(self) -> None:
        self._services: Dict[str, ServiceInfo] = {}

    def register_service(
        self,
        service_id: str,
        node_id: str,
        status: str,
        pid: Optional[int] = None,
        command: Optional[str] = None,
    ) -> ServiceInfo:
        """Register a service or update an existing service."""

        if not service_id:
            raise ValueError("service_id is required")

        if not node_id:
            raise ValueError("node_id is required")

        if not status:
            raise ValueError("status is required")

        service = ServiceInfo(
            service_id=service_id,
            node_id=node_id,
            status=status,
            pid=pid,
            command=command,
        )

        self._services[service_id] = service

        return service

    def get_service(
        self,
        service_id: str,
    ) -> Optional[ServiceInfo]:
        """Return a service by ID."""

        return self._services.get(service_id)

    def list_services(self) -> List[ServiceInfo]:
        """Return all registered services."""

        return list(self._services.values())

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
        status: Optional[str] = None,
        pid: Optional[int] = None,
    ) -> ServiceInfo:
        """Update the runtime state of a service."""

        service = self._services.get(service_id)

        if service is None:
            raise KeyError(
                f"Service {service_id} not found"
            )

        if status is not None:
            service.status = status

        if pid is not None:
            service.pid = pid

        service.updated_at = datetime.now(timezone.utc)

        return service

    def remove_service(
        self,
        service_id: str,
    ) -> Optional[ServiceInfo]:
        """Remove a service from the registry."""

        return self._services.pop(service_id, None)

    def remove_node_services(
        self,
        node_id: str,
    ) -> List[ServiceInfo]:
        """Remove all services assigned to a node."""

        removed: List[ServiceInfo] = []

        for service_id, service in list(self._services.items()):
            if service.node_id == node_id:
                removed.append(
                    self._services.pop(service_id)
                )

        return removed

    def service_count(self) -> int:
        """Return the number of registered services."""

        return len(self._services)

    def clear(self) -> None:
        """Clear all registered services."""

        self._services.clear()