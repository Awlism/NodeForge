"""Resource reservation and accounting for NodeForge."""

from __future__ import annotations

import math
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

    @staticmethod
    def _validate_service_id(
        service_id: str,
    ) -> None:
        if (
            not isinstance(
                service_id,
                str,
            )
            or not service_id.strip()
        ):
            raise ValueError(
                "service_id is required"
            )

    @staticmethod
    def _validate_node_id(
        node_id: str,
    ) -> None:
        if (
            not isinstance(
                node_id,
                str,
            )
            or not node_id.strip()
        ):
            raise ValueError(
                "node_id is required"
            )

    @staticmethod
    def _validate_cpu(
        cpu_cores: float,
    ) -> None:
        if (
            not isinstance(
                cpu_cores,
                (int, float),
            )
            or isinstance(
                cpu_cores,
                bool,
            )
            or not math.isfinite(
                float(cpu_cores)
            )
            or cpu_cores < 0
        ):
            raise ValueError(
                "cpu_cores must be a "
                "finite non-negative number"
            )

    @staticmethod
    def _validate_memory(
        memory_mb: int,
    ) -> None:
        if (
            not isinstance(
                memory_mb,
                int,
            )
            or isinstance(
                memory_mb,
                bool,
            )
            or memory_mb < 0
        ):
            raise ValueError(
                "memory_mb must be a "
                "non-negative integer"
            )

    @staticmethod
    def _validate_disk(
        disk_gb: float,
    ) -> None:
        if (
            not isinstance(
                disk_gb,
                (int, float),
            )
            or isinstance(
                disk_gb,
                bool,
            )
            or not math.isfinite(
                float(disk_gb)
            )
            or disk_gb < 0
        ):
            raise ValueError(
                "disk_gb must be a "
                "finite non-negative number"
            )

    def reserve(
        self,
        service_id: str,
        node_id: str,
        cpu_cores: float = 0.0,
        memory_mb: int = 0,
        disk_gb: float = 0.0,
    ) -> ResourceReservation:
        """Create or replace a service reservation."""

        self._validate_service_id(
            service_id
        )

        self._validate_node_id(
            node_id
        )

        self._validate_cpu(
            cpu_cores
        )

        self._validate_memory(
            memory_mb
        )

        self._validate_disk(
            disk_gb
        )

        reservation = ResourceReservation(
            service_id=service_id,
            node_id=node_id,
            cpu_cores=float(cpu_cores),
            memory_mb=memory_mb,
            disk_gb=float(disk_gb),
        )

        self._reservations[
            service_id
        ] = reservation

        return reservation

    def get(
        self,
        service_id: str,
    ) -> ResourceReservation | None:
        """Return a service reservation."""

        return self._reservations.get(
            service_id
        )

    def release(
        self,
        service_id: str,
    ) -> ResourceReservation | None:
        """Release a service reservation."""

        return self._reservations.pop(
            service_id,
            None,
        )

    def move(
        self,
        service_id: str,
        target_node_id: str,
    ) -> ResourceReservation:
        """Move an existing reservation to another node."""

        reservation = self._reservations.get(
            service_id
        )

        if reservation is None:
            raise KeyError(
                f"Reservation for {service_id} not found"
            )

        self._validate_node_id(
            target_node_id
        )

        moved = ResourceReservation(
            service_id=reservation.service_id,
            node_id=target_node_id,
            cpu_cores=reservation.cpu_cores,
            memory_mb=reservation.memory_mb,
            disk_gb=reservation.disk_gb,
        )

        self._reservations[
            service_id
        ] = moved

        return moved

    def restore(
        self,
        reservations: list[
            ResourceReservation
        ],
    ) -> None:
        """Restore reservations after controller restart."""

        restored: Dict[
            str,
            ResourceReservation,
        ] = {}

        for reservation in reservations:
            self._validate_service_id(
                reservation.service_id
            )

            self._validate_node_id(
                reservation.node_id
            )

            self._validate_cpu(
                reservation.cpu_cores
            )

            self._validate_memory(
                reservation.memory_mb
            )

            self._validate_disk(
                reservation.disk_gb
            )

            if (
                reservation.service_id
                in restored
            ):
                raise ValueError(
                    "Duplicate service reservation: "
                    f"{reservation.service_id}"
                )

            restored[
                reservation.service_id
            ] = reservation

        self._reservations = restored

    def list_reservations(
        self,
    ) -> list[ResourceReservation]:
        """Return all reservations."""

        return list(
            self._reservations.values()
        )

    def list_node_reservations(
        self,
        node_id: str,
    ) -> list[ResourceReservation]:
        """Return reservations belonging to a node."""

        self._validate_node_id(
            node_id
        )

        return [
            reservation
            for reservation in (
                self._reservations.values()
            )
            if reservation.node_id == node_id
        ]

    def node_usage(
        self,
        node_id: str,
    ) -> dict[str, float | int]:
        """Return reserved resources for a node."""

        reservations = (
            self.list_node_reservations(
                node_id
            )
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
        """Return total or per-node reservation count."""

        if node_id is None:
            return len(
                self._reservations
            )

        return len(
            self.list_node_reservations(
                node_id
            )
        )

    def clear(
        self,
    ) -> None:
        """Clear all reservations."""

        self._reservations.clear()