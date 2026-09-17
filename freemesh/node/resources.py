"""Node resource information for NodeForge."""

from dataclasses import dataclass


@dataclass
class NodeResources:
    """Represent the available and currently used resources of a node."""

    cpu_cores: float
    cpu_usage_percent: float
    memory_total_mb: int
    memory_used_mb: int
    disk_total_gb: float
    disk_used_gb: float
    running_services: int = 0

    @property
    def memory_available_mb(self) -> int:
        """Return available memory in megabytes."""

        return max(
            self.memory_total_mb - self.memory_used_mb,
            0,
        )

    @property
    def disk_available_gb(self) -> float:
        """Return available disk space in gigabytes."""

        return max(
            self.disk_total_gb - self.disk_used_gb,
            0.0,
        )

    @property
    def memory_usage_percent(self) -> float:
        """Return current memory usage percentage."""

        if self.memory_total_mb <= 0:
            return 100.0

        return (
            self.memory_used_mb
            / self.memory_total_mb
        ) * 100.0

    @property
    def disk_usage_percent(self) -> float:
        """Return current disk usage percentage."""

        if self.disk_total_gb <= 0:
            return 100.0

        return (
            self.disk_used_gb
            / self.disk_total_gb
        ) * 100.0

    def has_capacity(
        self,
        required_cpu_cores: float = 0.0,
        required_memory_mb: int = 0,
        required_disk_gb: float = 0.0,
        max_cpu_usage_percent: float = 90.0,
        max_memory_usage_percent: float = 90.0,
    ) -> bool:
        """Return whether the node can accept a new service."""

        if required_cpu_cores < 0:
            raise ValueError(
                "required_cpu_cores cannot be negative"
            )

        if required_memory_mb < 0:
            raise ValueError(
                "required_memory_mb cannot be negative"
            )

        if required_disk_gb < 0:
            raise ValueError(
                "required_disk_gb cannot be negative"
            )

        if not 0 <= max_cpu_usage_percent <= 100:
            raise ValueError(
                "max_cpu_usage_percent must be between 0 and 100"
            )

        if not 0 <= max_memory_usage_percent <= 100:
            raise ValueError(
                "max_memory_usage_percent must be between 0 and 100"
            )

        cpu_available = (
            self.cpu_cores * (
                max_cpu_usage_percent / 100.0
            )
        )

        memory_available = (
            self.memory_total_mb
            * (max_memory_usage_percent / 100.0)
            - self.memory_used_mb
        )

        disk_available = self.disk_available_gb

        return (
            self.cpu_usage_percent
            <= max_cpu_usage_percent
            and required_cpu_cores <= (
                cpu_available
                - (
                    self.cpu_cores
                    * self.cpu_usage_percent
                    / 100.0
                )
            )
            and required_memory_mb <= memory_available
            and required_disk_gb <= disk_available
        )