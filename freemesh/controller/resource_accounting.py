"""Resource reservation and accounting for NodeForge."""

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ResourceReservation:
    """Resources reserved by a service on a node."""

    service_id: str
    node_id: str
    cpu_cores: float
    memory_mb: int
    disk_gb: float


class ResourceAccounting:
    """Track resource reservations independently from live reports."""

    def __init__(self) -> None:
        self._reservations: Dict[
            str,
            ResourceReservation,
        ] = {}

    def reserve(
        self,
        service_id: str,
        node_id: str,
        cpu_cores: float = 0.0,
        memory_mb: int = 0,
        disk_gb: float = 0.0,
    ) -> ResourceReservation:
        if not service_id:
            raise ValueError("service_id is required")

        if not node_id:
            raise ValueError("node_id is required")

        if cpu_cores < 0:
            raise ValueError(
                "cpu_cores cannot be negative"
            )

        if memory_mb < 0:
            raise ValueError(
                "memory_mb cannot be negative"
            )

        if disk_gb < 0:
            raise ValueError(
                "disk_gb cannot be negative"
            )

        reservation = ResourceReservation(
            service_id=service_id,
            node_id=node_id,
            cpu_cores=cpu_cores,
            memory_mb=memory_mb,
            disk_gb=disk_gb,
        )

        self._reservations[service_id] = reservation

        return reservation

    def get(
        self,
        service_id: str,
    ) -> ResourceReservation | None:
        return self._reservations.get(service_id)

    def release(
        self,
        service_id: str,
    ) -> ResourceReservation | None:
        return self._reservations.pop(
            service_id,
            None,
        )

    def move(
        self,
        service_id: str,
        target_node_id: str,
    ) -> ResourceReservation:
        reservation = self._reservations.get(
            service_id
        )

        if reservation is None:
            raise KeyError(
                f"Reservation for {service_id} not found"
            )

        if not target_node_id:
            raise ValueError(
                "target_node_id is required"
            )

        moved = ResourceReservation(
            service_id=reservation.service_id,
            node_id=target_node_id,
            cpu_cores=reservation.cpu_cores,
            memory_mb=reservation.memory_mb,
            disk_gb=reservation.disk_gb,
        )

        self._reservations[service_id] = moved

        return moved

    def list_reservations(
        self,
    ) -> list[ResourceReservation]:
        return list(
            self._reservations.values()
        )

    def list_node_reservations(
        self,
        node_id: str,
    ) -> list[ResourceReservation]:
        return [
            reservation
            for reservation in self._reservations.values()
            if reservation.node_id == node_id
        ]

    def node_usage(
        self,
        node_id: str,
    ) -> dict[str, float | int]:
        reservations = self.list_node_reservations(
            node_id
        )

        return {
            "cpu_cores": sum(
                item.cpu_cores
                for item in reservations
            ),
            "memory_mb": sum(
                item.memory_mb
                for item in reservations
            ),
            "disk_gb": sum(
                item.disk_gb
                for item in reservations
            ),
        }

    def service_count(
        self,
        node_id: str | None = None,
    ) -> int:
        if node_id is None:
            return len(self._reservations)

        return len(
            self.list_node_reservations(node_id)
        )

    def clear(self) -> None:
        self._reservations.clear()