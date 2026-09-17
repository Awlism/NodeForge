"""Resource-aware scheduling for NodeForge."""

from dataclasses import dataclass
from typing import Iterable, Optional

from freemesh.node.resources import NodeResources
from freemesh.scheduler.scheduler import NodeCandidate


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

    def select_node(
        self,
        nodes: Iterable[ResourceNodeCandidate],
        required_cpu_cores: float = 0.0,
        required_memory_mb: int = 0,
        required_disk_gb: float = 0.0,
        exclude_node_id: Optional[str] = None,
    ) -> Optional[ResourceNodeCandidate]:
        """Select the most suitable node with enough resources."""

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

        candidates = []

        for node in nodes:
            if not node.available:
                continue

            if node.node_id == exclude_node_id:
                continue

            if node.resources is None:
                continue

            if not node.resources.has_capacity(
                required_cpu_cores=required_cpu_cores,
                required_memory_mb=required_memory_mb,
                required_disk_gb=required_disk_gb,
                max_cpu_usage_percent=self.max_cpu_usage_percent,
                max_memory_usage_percent=(
                    self.max_memory_usage_percent
                ),
            ):
                continue

            candidates.append(node)

        if not candidates:
            return None

        return min(
            candidates,
            key=self._node_load_score,
        )

    @staticmethod
    def _node_load_score(
        node: ResourceNodeCandidate,
    ) -> tuple[float, float, int]:
        """Return a score used to prefer less-loaded nodes."""

        if node.resources is None:
            return (100.0, 100.0, node.running_services)

        return (
            node.resources.cpu_usage_percent,
            node.resources.memory_usage_percent,
            node.running_services,
        )