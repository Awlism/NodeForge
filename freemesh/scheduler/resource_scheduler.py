"""Resource-aware scheduling for NodeForge."""

from dataclasses import dataclass
from typing import Iterable, Optional

from freemesh.controller.resource_accounting import ResourceAccounting
from freemesh.node.resources import NodeResources
from freemesh.scheduler.scheduler import NodeCandidate
from freemesh.service_requirements import ServiceRequirements


@dataclass
class ResourceNodeCandidate(NodeCandidate):
    """A node candidate with resource information."""

    resources: Optional[NodeResources] = None


class ResourceScheduler:
    """Select nodes based on availability and resource capacity."""

    def __init__(
        self,
        max_cpu_usage_percent: float = 90.0,
        max_memory_usage_percent: float = 90.0,
        accounting: Optional[ResourceAccounting] = None,
    ) -> None:
        if not 0 <= max_cpu_usage_percent <= 100:
            raise ValueError(
                "max_cpu_usage_percent must be between 0 and 100"
            )

        if not 0 <= max_memory_usage_percent <= 100:
            raise ValueError(
                "max_memory_usage_percent must be between 0 and 100"
            )

        self.max_cpu_usage_percent = max_cpu_usage_percent
        self.max_memory_usage_percent = max_memory_usage_percent
        self.accounting = accounting

    def select_node(
        self,
        nodes: Iterable[ResourceNodeCandidate],
        required_cpu_cores: float = 0.0,
        required_memory_mb: int = 0,
        required_disk_gb: float = 0.0,
        exclude_node_id: Optional[str] = None,
    ) -> Optional[ResourceNodeCandidate]:
        requirements = ServiceRequirements(
            cpu_cores=required_cpu_cores,
            memory_mb=required_memory_mb,
            disk_gb=required_disk_gb,
        )

        return self.select_node_for_requirements(
            nodes=nodes,
            requirements=requirements,
            exclude_node_id=exclude_node_id,
        )

    def select_node_for_requirements(
        self,
        nodes: Iterable[ResourceNodeCandidate],
        requirements: ServiceRequirements,
        exclude_node_id: Optional[str] = None,
    ) -> Optional[ResourceNodeCandidate]:
        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

        candidates = []

        for node in nodes:
            if not node.available:
                continue

            if node.node_id == exclude_node_id:
                continue

            if node.resources is None:
                continue

            if not self._has_capacity(
                node,
                requirements,
            ):
                continue

            candidates.append(node)

        if not candidates:
            return None

        return min(
            candidates,
            key=self._node_load_score,
        )

    def _has_capacity(
        self,
        node: ResourceNodeCandidate,
        requirements: ServiceRequirements,
    ) -> bool:
        """Check live capacity while respecting reservations."""

        resources = node.resources

        if resources is None:
            return False

        if self.accounting is None:
            return resources.has_capacity(
                required_cpu_cores=requirements.cpu_cores,
                required_memory_mb=requirements.memory_mb,
                required_disk_gb=requirements.disk_gb,
                max_cpu_usage_percent=(
                    self.max_cpu_usage_percent
                ),
                max_memory_usage_percent=(
                    self.max_memory_usage_percent
                ),
            )

        usage = self.accounting.node_usage(
            node.node_id
        )

        max_cpu_cores = (
            resources.cpu_cores
            * self.max_cpu_usage_percent
            / 100.0
        )

        live_cpu_cores = (
            resources.cpu_cores
            * resources.cpu_usage_percent
            / 100.0
        )

        reserved_cpu_cores = float(
            usage.get("cpu_cores", 0.0)
        )

        effective_cpu_cores = max(
            live_cpu_cores,
            reserved_cpu_cores,
        )

        effective_memory_mb = max(
            resources.memory_used_mb,
            int(usage.get("memory_mb", 0)),
        )

        effective_disk_gb = max(
            resources.disk_used_gb,
            float(usage.get("disk_gb", 0.0)),
        )

        cpu_available = max(
            max_cpu_cores - effective_cpu_cores,
            0.0,
        )

        memory_available = max(
            resources.memory_total_mb
            * self.max_memory_usage_percent
            / 100.0
            - effective_memory_mb,
            0.0,
        )

        disk_available = max(
            resources.disk_total_gb
            - effective_disk_gb,
            0.0,
        )

        return (
            resources.cpu_usage_percent
            <= self.max_cpu_usage_percent
            and requirements.cpu_cores
            <= cpu_available
            and requirements.memory_mb
            <= memory_available
            and requirements.disk_gb
            <= disk_available
        )

    @staticmethod
    def _node_load_score(
        node: ResourceNodeCandidate,
    ) -> tuple[float, float, int]:
        if node.resources is None:
            return (
                100.0,
                100.0,
                node.running_services,
            )

        return (
            node.resources.cpu_usage_percent,
            node.resources.memory_usage_percent,
            node.running_services,
        )