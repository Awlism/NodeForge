"""Persistent node registry for the NodeForge controller."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
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

    def is_offline(
        self,
        timeout_seconds: float,
    ) -> bool:
        """Check whether heartbeat timeout has elapsed."""

        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be positive"
            )

        reference_time = (
            self.last_heartbeat_time
            or self.registration_time
        )

        elapsed = (
            datetime.now(timezone.utc)
            - reference_time
        ).total_seconds()

        return elapsed > timeout_seconds


class NodeRegistry:
    """Store and manage node state with SQLite persistence."""

    def __init__(
        self,
        database_path: str = ":memory:",
    ) -> None:
        self._nodes: Dict[
            str,
            NodeInfo,
        ] = {}

        self._database_path = database_path

        self._connection = sqlite3.connect(
            database_path,
            timeout=30.0,
            isolation_level=None,
        )

        self._connection.row_factory = (
            sqlite3.Row
        )

        self._configure_database()
        self._create_tables()
        self._load_from_store()

    def _configure_database(
        self,
    ) -> None:
        """Configure SQLite for resilient controller state."""

        self._connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        self._connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        if self._database_path != ":memory:":
            self._connection.execute(
                "PRAGMA journal_mode = WAL"
            )

        self._connection.execute(
            "PRAGMA synchronous = NORMAL"
        )

    def _create_tables(
        self,
    ) -> None:
        """Create the persistent node table."""

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS node_registry (
                node_id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL,
                registration_time TEXT NOT NULL,
                last_heartbeat_time TEXT,
                state TEXT NOT NULL,
                connection_address TEXT,
                connection_port INTEGER,
                authenticated INTEGER NOT NULL
            )
            """
        )

    @staticmethod
    def _datetime_to_string(
        value: Optional[datetime],
    ) -> Optional[str]:
        if value is None:
            return None

        return value.isoformat()

    @staticmethod
    def _string_to_datetime(
        value: Optional[str],
    ) -> Optional[datetime]:
        if value is None:
            return None

        parsed = datetime.fromisoformat(
            value
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        return parsed

    @staticmethod
    def _state_to_string(
        state: NodeState | str,
    ) -> str:
        if isinstance(
            state,
            NodeState,
        ):
            return state.value

        return str(state)

    @staticmethod
    def _normalize_state(
        state: NodeState | str,
    ) -> NodeState:
        if isinstance(
            state,
            NodeState,
        ):
            return state

        return NodeState(
            str(state)
        )

    def _persist_node(
        self,
        node_info: NodeInfo,
    ) -> None:
        """Persist one node atomically."""

        self._connection.execute(
            """
            INSERT INTO node_registry (
                node_id,
                hostname,
                registration_time,
                last_heartbeat_time,
                state,
                connection_address,
                connection_port,
                authenticated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                hostname = excluded.hostname,
                registration_time = excluded.registration_time,
                last_heartbeat_time = excluded.last_heartbeat_time,
                state = excluded.state,
                connection_address = excluded.connection_address,
                connection_port = excluded.connection_port,
                authenticated = excluded.authenticated
            """,
            (
                node_info.node_id,
                node_info.hostname,
                self._datetime_to_string(
                    node_info.registration_time
                ),
                self._datetime_to_string(
                    node_info.last_heartbeat_time
                ),
                self._state_to_string(
                    node_info.state
                ),
                node_info.connection_address,
                node_info.connection_port,
                int(
                    node_info.authenticated
                ),
            ),
        )

    def _load_from_store(
        self,
    ) -> None:
        """Load persisted nodes."""

        rows = self._connection.execute(
            """
            SELECT
                node_id,
                hostname,
                registration_time,
                last_heartbeat_time,
                state,
                connection_address,
                connection_port,
                authenticated
            FROM node_registry
            """
        ).fetchall()

        for row in rows:
            node_info = NodeInfo(
                node_id=row["node_id"],
                hostname=row["hostname"],
                registration_time=(
                    self._string_to_datetime(
                        row[
                            "registration_time"
                        ]
                    )
                ),
                last_heartbeat_time=(
                    self._string_to_datetime(
                        row[
                            "last_heartbeat_time"
                        ]
                    )
                ),
                state=self._normalize_state(
                    row["state"]
                ),
                connection_address=row[
                    "connection_address"
                ],
                connection_port=row[
                    "connection_port"
                ],
                authenticated=bool(
                    row["authenticated"]
                ),
            )

            self._nodes[
                node_info.node_id
            ] = node_info

    def register_node(
        self,
        node_id: str,
        hostname: str,
        connection_address: Optional[str] = None,
        connection_port: Optional[int] = None,
    ) -> NodeInfo:
        """Register a node or refresh an existing node."""

        if (
            not isinstance(
                node_id,
                str,
            )
            or not node_id.strip()
        ):
            raise ValueError(
                "node_id is required"
            )

        if (
            not isinstance(
                hostname,
                str,
            )
            or not hostname.strip()
        ):
            raise ValueError(
                "hostname is required"
            )

        if (
            connection_port is not None
            and (
                not isinstance(
                    connection_port,
                    int,
                )
                or isinstance(
                    connection_port,
                    bool,
                )
                or not (
                    1
                    <= connection_port
                    <= 65535
                )
            )
        ):
            raise ValueError(
                "connection_port must be between 1 and 65535"
            )

        now = datetime.now(
            timezone.utc
        )

        existing_node = self._nodes.get(
            node_id
        )

        if existing_node is not None:
            existing_node.hostname = hostname
            existing_node.registration_time = now
            existing_node.last_heartbeat_time = None
            existing_node.connection_address = (
                connection_address
            )
            existing_node.connection_port = (
                connection_port
            )
            existing_node.state = (
                NodeState.REGISTERING
            )
            existing_node.authenticated = False

            self._persist_node(
                existing_node
            )

            return existing_node

        node_info = NodeInfo(
            node_id=node_id,
            hostname=hostname,
            registration_time=now,
            connection_address=(
                connection_address
            ),
            connection_port=(
                connection_port
            ),
            state=NodeState.REGISTERING,
        )

        self._nodes[
            node_id
        ] = node_info

        try:
            self._persist_node(
                node_info
            )
        except Exception:
            self._nodes.pop(
                node_id,
                None,
            )
            raise

        return node_info

    def get_node(
        self,
        node_id: str,
    ) -> Optional[NodeInfo]:
        return self._nodes.get(
            node_id
        )

    def list_nodes(
        self,
    ) -> List[NodeInfo]:
        return list(
            self._nodes.values()
        )

    def update_node_state(
        self,
        node_id: str,
        state: NodeState | str,
    ) -> NodeInfo:
        if node_id not in self._nodes:
            raise KeyError(
                f"Node {node_id} not found in registry"
            )

        normalized = self._normalize_state(
            state
        )

        node_info = self._nodes[
            node_id
        ]

        previous = node_info.state
        node_info.state = normalized

        try:
            self._persist_node(
                node_info
            )
        except Exception:
            node_info.state = previous
            raise

        return node_info

    def authenticate_node(
        self,
        node_id: str,
        authenticated: bool = True,
    ) -> NodeInfo:
        if node_id not in self._nodes:
            raise KeyError(
                f"Node {node_id} not found in registry"
            )

        node_info = self._nodes[
            node_id
        ]

        previous_authenticated = (
            node_info.authenticated
        )
        previous_state = node_info.state

        node_info.authenticated = (
            authenticated
        )

        if authenticated:
            if (
                node_info.state
                == NodeState.REGISTERING
            ):
                node_info.state = (
                    NodeState.ONLINE
                )
        else:
            node_info.state = (
                NodeState.AUTH_FAILED
            )

        try:
            self._persist_node(
                node_info
            )
        except Exception:
            node_info.authenticated = (
                previous_authenticated
            )
            node_info.state = (
                previous_state
            )
            raise

        return node_info

    def record_heartbeat(
        self,
        node_id: str,
    ) -> NodeInfo:
        if node_id not in self._nodes:
            raise KeyError(
                f"Node {node_id} not found in registry"
            )

        node_info = self._nodes[
            node_id
        ]

        previous_time = (
            node_info.last_heartbeat_time
        )
        previous_state = node_info.state

        node_info.last_heartbeat_time = (
            datetime.now(timezone.utc)
        )

        if (
            node_info.state
            == NodeState.OFFLINE
        ):
            node_info.state = (
                NodeState.ONLINE
            )

        try:
            self._persist_node(
                node_info
            )
        except Exception:
            node_info.last_heartbeat_time = (
                previous_time
            )
            node_info.state = (
                previous_state
            )
            raise

        return node_info

    def detect_offline_nodes(
        self,
        timeout_seconds: float,
    ) -> List[NodeInfo]:
        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be positive"
            )

        return [
            node_info
            for node_info in self._nodes.values()
            if (
                node_info.state
                != NodeState.OFFLINE
                and node_info.is_offline(
                    timeout_seconds
                )
            )
        ]

    def mark_offline(
        self,
        node_id: str,
    ) -> NodeInfo:
        if node_id not in self._nodes:
            raise KeyError(
                f"Node {node_id} not found in registry"
            )

        node_info = self._nodes[
            node_id
        ]

        previous = node_info.state
        node_info.state = NodeState.OFFLINE

        try:
            self._persist_node(
                node_info
            )
        except Exception:
            node_info.state = previous
            raise

        return node_info

    def unregister_node(
        self,
        node_id: str,
    ) -> Optional[NodeInfo]:
        node_info = self._nodes.get(
            node_id
        )

        if node_info is None:
            return None

        self._connection.execute(
            """
            DELETE FROM node_registry
            WHERE node_id = ?
            """,
            (node_id,),
        )

        self._nodes.pop(
            node_id,
            None,
        )

        return node_info

    def clear(
        self,
    ) -> None:
        self._connection.execute(
            "DELETE FROM node_registry"
        )

        self._nodes.clear()

    def node_count(
        self,
    ) -> int:
        return len(
            self._nodes
        )

    def close(
        self,
    ) -> None:
        self._connection.close()