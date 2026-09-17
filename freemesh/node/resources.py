"""Node resource information and collection for NodeForge."""

import os
import shutil
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
            self.cpu_cores
            * (max_cpu_usage_percent / 100.0)
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
            and required_cpu_cores
            <= (
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


def _read_linux_memory() -> tuple[int, int]:
    """Read total and available memory from Linux /proc."""

    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as file:
            values = {}

            for line in file:
                parts = line.split()

                if len(parts) >= 2:
                    key = parts[0].rstrip(":")
                    values[key] = int(parts[1])

        total_kb = values.get("MemTotal", 0)
        available_kb = values.get(
            "MemAvailable",
            values.get("MemFree", 0),
        )

        total_mb = total_kb // 1024
        available_mb = available_kb // 1024

        used_mb = max(
            total_mb - available_mb,
            0,
        )

        return total_mb, used_mb

    except (OSError, ValueError):
        return 0, 0


def _read_linux_cpu_usage() -> float:
    """Estimate CPU usage from Linux /proc/stat."""

    try:
        with open("/proc/stat", "r", encoding="utf-8") as file:
            first_line = file.readline()

        parts = first_line.split()

        if not parts or parts[0] != "cpu":
            return 0.0

        values = [int(value) for value in parts[1:]]

        if len(values) < 4:
            return 0.0

        idle = values[3]

        if len(values) > 4:
            idle += values[4]

        total = sum(values)

        if total <= 0:
            return 0.0

        usage = (
            (total - idle)
            / total
        ) * 100.0

        return max(
            0.0,
            min(usage, 100.0),
        )

    except (OSError, ValueError):
        return 0.0


def collect_node_resources(
    running_services: int = 0,
) -> NodeResources:
    """Collect the current resources of the local node."""

    if running_services < 0:
        raise ValueError(
            "running_services cannot be negative"
        )

    cpu_cores = float(
        os.cpu_count() or 1
    )

    cpu_usage_percent = _read_linux_cpu_usage()

    memory_total_mb, memory_used_mb = (
        _read_linux_memory()
    )

    disk = shutil.disk_usage("/")

    disk_total_gb = (
        disk.total
        / (1024 ** 3)
    )

    disk_used_gb = (
        disk.used
        / (1024 ** 3)
    )

    return NodeResources(
        cpu_cores=cpu_cores,
        cpu_usage_percent=cpu_usage_percent,
        memory_total_mb=memory_total_mb,
        memory_used_mb=memory_used_mb,
        disk_total_gb=disk_total_gb,
        disk_used_gb=disk_used_gb,
        running_services=running_services,
    )