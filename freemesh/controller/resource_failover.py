"""Resource-aware failover and migration for NodeForge."""

from dataclasses import dataclass
from typing import Iterable, Optional

from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
    ResourceScheduler,
)
from freemesh.service_requirements import ServiceRequirements


@dataclass(frozen=True)
class MigrationPlan:
    """Describe how a service should migrate to another node."""

    service_id: str
    source_node_id: str
    target_node_id: str
    command: str
    requirements: ServiceRequirements


class ResourceFailover:
    """Select resource-capable replacement nodes for failed services."""

    def __init__(
        self,
        scheduler: Optional[ResourceScheduler] = None,
    ) -> None:
        self.scheduler = scheduler or ResourceScheduler()

    def select_replacement_node(
        self,
        nodes: Iterable[ResourceNodeCandidate],
        failed_node_id: str,
        requirements: ServiceRequirements,
    ) -> Optional[ResourceNodeCandidate]:
        """Select a replacement node with enough capacity."""

        if not failed_node_id:
            raise ValueError(
                "failed_node_id is required"
            )

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

        return self.scheduler.select_node_for_requirements(
            nodes=nodes,
            requirements=requirements,
            exclude_node_id=failed_node_id,
        )

    def create_migration_plan(
        self,
        service_id: str,
        source_node_id: str,
        command: str,
        requirements: ServiceRequirements,
        nodes: Iterable[ResourceNodeCandidate],
    ) -> Optional[MigrationPlan]:
        """Create a migration plan for a failed service."""

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        if not source_node_id:
            raise ValueError(
                "source_node_id is required"
            )

        if not command:
            raise ValueError(
                "command is required"
            )

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

        replacement = self.select_replacement_node(
            nodes=nodes,
            failed_node_id=source_node_id,
            requirements=requirements,
        )

        if replacement is None:
            return None

        return MigrationPlan(
            service_id=service_id,
            source_node_id=source_node_id,
            target_node_id=replacement.node_id,
            command=command,
            requirements=requirements,
        )