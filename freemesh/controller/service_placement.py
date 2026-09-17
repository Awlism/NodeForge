"""Service placement coordination for NodeForge."""

from dataclasses import dataclass
from typing import Iterable, Optional

from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
    ResourceScheduler,
)
from freemesh.service_requirements import ServiceRequirements


@dataclass(frozen=True)
class PlacementResult:
    """Result of selecting a node for a service."""

    service_id: str
    node_id: str
    requirements: ServiceRequirements


class ServicePlacement:
    """Select an appropriate node for running a service."""

    def __init__(
        self,
        scheduler: Optional[ResourceScheduler] = None,
    ) -> None:
        self.scheduler = scheduler or ResourceScheduler()

    def select_node(
        self,
        service_id: str,
        requirements: ServiceRequirements,
        nodes: Iterable[ResourceNodeCandidate],
        exclude_node_id: Optional[str] = None,
    ) -> Optional[PlacementResult]:
        if not service_id:
            raise ValueError("service_id is required")

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

        selected = self.scheduler.select_node_for_requirements(
            nodes=nodes,
            requirements=requirements,
            exclude_node_id=exclude_node_id,
        )

        if selected is None:
            return None

        return PlacementResult(
            service_id=service_id,
            node_id=selected.node_id,
            requirements=requirements,
        )