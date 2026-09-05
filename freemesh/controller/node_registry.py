"""Node registry for the NodeForge controller."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


class NodeState(str, Enum):
    """Enumeration of possible node states."""

    UNKNOWN = "unknown"
    REGISTERING = "registering"
    ONLINE = "online"
    OFFLINE = "offline"
    AUTH_FAILED = "auth_failed"


@dataclass
class NodeInfo:
    """Information about a registered node."""

    node_id: str
    hostname: str
    registration_time: datetime
    last_heartbeat_time: Optional[datetime] = None
    state: NodeState = NodeState.UNKNOWN
    connection_address: Optional[str] = None
    connection_port: Optional[int] = None
    authenticated: bool = False

    def is_offline(self, timeout_seconds: float) -> bool:
        """Check if node is offline based on heartbeat timeout.

        Args:
            timeout_seconds: Seconds without heartbeat before considering offline

        Returns:
            True if node has not sent heartbeat within timeout period, False otherwise
        """
        if self.last_heartbeat_time is None:
            # No heartbeat received yet, check registration time
            elapsed = (datetime.now(timezone.utc) - self.registration_time).total_seconds()
            return elapsed > timeout_seconds

        elapsed = (datetime.now(timezone.utc) - self.last_heartbeat_time).total_seconds()
        return elapsed > timeout_seconds


class NodeRegistry:
    """In-memory registry for managing node state in the controller."""

    def __init__(self):
        """Initialize the node registry."""
        self._nodes: Dict[str, NodeInfo] = {}

    def register_node(
        self,
        node_id: str,
        hostname: str,
        connection_address: Optional[str] = None,
        connection_port: Optional[int] = None,
    ) -> NodeInfo:
        """Register a new node in the registry.

        Args:
            node_id: Unique identifier for the node
            hostname: Hostname or name of the node
            connection_address: IP address or hostname where node is reachable
            connection_port: Port where node is listening

        Returns:
            NodeInfo object for the registered node

        Raises:
            ValueError: If node_id is already registered
        """
        if node_id in self._nodes:
            raise ValueError(f"Node {node_id} is already registered")

        node_info = NodeInfo(
            node_id=node_id,
            hostname=hostname,
            registration_time=datetime.now(timezone.utc),
            connection_address=connection_address,
            connection_port=connection_port,
            state=NodeState.REGISTERING,
        )

        self._nodes[node_id] = node_info
        return node_info

    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get node information by node ID.

        Args:
            node_id: Unique identifier for the node

        Returns:
            NodeInfo if found, None otherwise
        """
        return self._nodes.get(node_id)

    def list_nodes(self) -> List[NodeInfo]:
        """List all registered nodes.

        Returns:
            List of all NodeInfo objects
        """
        return list(self._nodes.values())

    def update_node_state(self, node_id: str, state: NodeState) -> NodeInfo:
        """Update the state of a node.

        Args:
            node_id: Unique identifier for the node
            state: New NodeState for the node

        Returns:
            Updated NodeInfo object

        Raises:
            KeyError: If node is not registered
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id} not found in registry")

        node_info = self._nodes[node_id]
        node_info.state = state
        return node_info

    def authenticate_node(self, node_id: str, authenticated: bool = True) -> NodeInfo:
        """Update authentication status of a node.

        Args:
            node_id: Unique identifier for the node
            authenticated: Whether the node is authenticated

        Returns:
            Updated NodeInfo object

        Raises:
            KeyError: If node is not registered
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id} not found in registry")

        node_info = self._nodes[node_id]
        node_info.authenticated = authenticated

        # Update state based on authentication result
        if authenticated:
            if node_info.state == NodeState.REGISTERING:
                node_info.state = NodeState.ONLINE
        else:
            node_info.state = NodeState.AUTH_FAILED

        return node_info

    def record_heartbeat(self, node_id: str) -> NodeInfo:
        """Record a heartbeat from a node.

        Args:
            node_id: Unique identifier for the node

        Returns:
            Updated NodeInfo object

        Raises:
            KeyError: If node is not registered
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id} not found in registry")

        node_info = self._nodes[node_id]
        node_info.last_heartbeat_time = datetime.now(timezone.utc)

        # Update state to ONLINE if it was OFFLINE
        if node_info.state == NodeState.OFFLINE:
            node_info.state = NodeState.ONLINE

        return node_info

    def detect_offline_nodes(self, timeout_seconds: float) -> List[NodeInfo]:
        """Detect nodes that have not sent heartbeats within the timeout period.

        Args:
            timeout_seconds: Seconds without heartbeat before considering offline

        Returns:
            List of NodeInfo objects that are considered offline
        """
        offline_nodes = []

        for node_info in self._nodes.values():
            if node_info.state != NodeState.OFFLINE and node_info.is_offline(timeout_seconds):
                offline_nodes.append(node_info)

        return offline_nodes

    def mark_offline(self, node_id: str) -> NodeInfo:
        """Mark a node as offline.

        Args:
            node_id: Unique identifier for the node

        Returns:
            Updated NodeInfo object

        Raises:
            KeyError: If node is not registered
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id} not found in registry")

        node_info = self._nodes[node_id]
        node_info.state = NodeState.OFFLINE
        return node_info

    def unregister_node(self, node_id: str) -> Optional[NodeInfo]:
        """Unregister a node from the registry.

        Args:
            node_id: Unique identifier for the node

        Returns:
            NodeInfo of the unregistered node, or None if not found
        """
        return self._nodes.pop(node_id, None)

    def clear(self) -> None:
        """Clear all nodes from the registry."""
        self._nodes.clear()

    def node_count(self) -> int:
        """Get the total number of registered nodes.

        Returns:
            Count of registered nodes
        """
        return len(self._nodes)
