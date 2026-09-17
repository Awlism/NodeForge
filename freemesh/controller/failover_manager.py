"""Failover coordination for NodeForge services."""

from typing import Iterable, Optional

from freemesh.scheduler.scheduler import NodeCandidate, Scheduler


class FailoverManager:
    """Select a healthy replacement node for a failed service."""

    def __init__(
        self,
        scheduler: Optional[Scheduler] = None,
    ) -> None:
        self.scheduler = scheduler or Scheduler()

    def select_replacement_node(
        self,
        nodes: Iterable[NodeCandidate],
        failed_node_id: str,
    ) -> Optional[NodeCandidate]:
        """Select a healthy node different from the failed node."""

        return self.scheduler.select_node(
            nodes,
            exclude_node_id=failed_node_id,
        )