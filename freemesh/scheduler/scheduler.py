"""Node scheduling for NodeForge."""

from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass
class NodeCandidate:
    """A node that can potentially run a service."""

    node_id: str
    available: bool = True
    running_services: int = 0


class Scheduler:
    """Select a suitable node for service execution."""

    def select_node(
        self,
        nodes: Iterable[NodeCandidate],
        exclude_node_id: Optional[str] = None,
    ) -> Optional[NodeCandidate]:
        candidates = [
            node
            for node in nodes
            if node.available
            and node.node_id != exclude_node_id
        ]

        if not candidates:
            return None

        return min(
            candidates,
            key=lambda node: node.running_services,
        )