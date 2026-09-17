"""Resource requirements for NodeForge services."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceRequirements:
    """Resources required by a service."""

    cpu_cores: float = 0.0
    memory_mb: int = 0
    disk_gb: float = 0.0

    def __post_init__(self) -> None:
        if self.cpu_cores < 0:
            raise ValueError(
                "cpu_cores cannot be negative"
            )

        if self.memory_mb < 0:
            raise ValueError(
                "memory_mb cannot be negative"
            )

        if self.disk_gb < 0:
            raise ValueError(
                "disk_gb cannot be negative"
            )

    def to_dict(self) -> dict[str, float | int]:
        return {
            "cpu_cores": self.cpu_cores,
            "memory_mb": self.memory_mb,
            "disk_gb": self.disk_gb,
        }

    @classmethod
    def from_dict(
        cls,
        payload: dict,
    ) -> "ServiceRequirements":
        return cls(
            cpu_cores=float(
                payload.get("cpu_cores", 0.0)
            ),
            memory_mb=int(
                payload.get("memory_mb", 0)
            ),
            disk_gb=float(
                payload.get("disk_gb", 0.0)
            ),
        )