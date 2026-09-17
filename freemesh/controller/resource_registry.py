"""Resource registry for the NodeForge controller."""

from typing import Dict, List, Optional

from freemesh.node.resources import NodeResources


class ResourceRegistry:
    """Store and manage resource information for NodeForge nodes."""

    def __init__(self) -> None:
        self._resources: Dict[str, NodeResources] = {}

    def register_resources(
        self,
        node_id: str,
        resources: NodeResources,
    ) -> NodeResources:
        """Register or replace resource information for a node."""

        if not node_id:
            raise ValueError("node_id is required")

        if not isinstance(resources, NodeResources):
            raise TypeError(
                "resources must be a NodeResources instance"
            )

        self._resources[node_id] = resources

        return resources

    def get_resources(
        self,
        node_id: str,
    ) -> Optional[NodeResources]:
        """Return resource information for a node."""

        return self._resources.get(node_id)

    def update_resources(
        self,
        node_id: str,
        resources: NodeResources,
    ) -> NodeResources:
        """Update resource information for an existing node."""

        if not node_id:
            raise ValueError("node_id is required")

        if not isinstance(resources, NodeResources):
            raise TypeError(
                "resources must be a NodeResources instance"
            )

        if node_id not in self._resources:
            raise KeyError(
                f"Resources for node {node_id} not found"
            )

        self._resources[node_id] = resources

        return resources

    def remove_resources(
        self,
        node_id: str,
    ) -> Optional[NodeResources]:
        """Remove resource information for a node."""

        return self._resources.pop(node_id, None)

    def list_resources(self) -> List[tuple[str, NodeResources]]:
        """Return resource information for all registered nodes."""

        return list(self._resources.items())

    def list_node_ids(self) -> List[str]:
        """Return IDs of all nodes with registered resources."""

        return list(self._resources.keys())

    def has_resources(
        self,
        node_id: str,
    ) -> bool:
        """Return whether resource information exists for a node."""

        return node_id in self._resources

    def node_count(self) -> int:
        """Return the number of nodes with registered resources."""

        return len(self._resources)

    def clear(self) -> None:
        """Remove all registered resource information."""

        self._resources.clear()