"""Controller for the NodeForge distributed system."""

import asyncio
import uuid
from typing import Optional, Dict

from freemesh.controller.node_registry import NodeRegistry, NodeState
from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport
from freemesh.security.auth import Authenticator, AuthenticationError


class Controller:
    """Central controller for managing NodeForge nodes.
    
    The controller listens for incoming node connections, manages node
    registration, authentication, heartbeat monitoring, and offline detection.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9999,
        heartbeat_timeout_seconds: float = 30.0,
        authenticator: Optional[Authenticator] = None,
    ):
        """Initialize the NodeForge controller.

        Args:
            host: Host address to listen on
            port: Port to listen on
            heartbeat_timeout_seconds: Seconds without heartbeat before marking node offline
            authenticator: Authenticator instance for node authentication
        """
        self.host = host
        self.port = port
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.authenticator = authenticator

        self.registry = NodeRegistry()
        self.server: Optional[asyncio.Server] = None
        self._running = False
        self._offline_detection_task: Optional[asyncio.Task] = None
        # Map node_id to transport connection for heartbeat responses
        self._active_nodes: Dict[str, TCPTransport] = {}

    async def start(self) -> None:
        """Start the controller and begin listening for node connections.

        Also starts the offline detection loop to monitor node health.

        Raises:
            RuntimeError: If the controller is already running
            OSError: If unable to bind to the specified host and port
        """
        if self.server is not None:
            raise RuntimeError("Controller is already running")

        self._running = True
        self.server = await asyncio.start_server(
            self._handle_client_connection,
            self.host,
            self.port,
        )

        # Start offline detection task
        self._offline_detection_task = asyncio.create_task(
            self._run_offline_detection()
        )

        async with self.server:
            await self.server.serve_forever()

    async def stop(self) -> None:
        """Stop the controller and close all connections.

        Closes the server socket, cancels offline detection, and closes
        all active client connections.
        """
        self._running = False

        if self._offline_detection_task:
            self._offline_detection_task.cancel()
            try:
                await self._offline_detection_task
            except asyncio.CancelledError:
                pass
            self._offline_detection_task = None

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        # Clean up active node connections
        for transport in self._active_nodes.values():
            try:
                await transport.disconnect()
            except Exception:
                pass
        self._active_nodes.clear()

    async def _handle_client_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle an incoming client connection.

        Implements the node lifecycle: registration → authentication → heartbeat.
        Handles message framing and transport protocol.

        Args:
            reader: AsyncIO stream reader for the connection
            writer: AsyncIO stream writer for the connection
        """
        transport = TCPTransport()
        transport.reader = reader
        transport.writer = writer
        node_id: Optional[str] = None

        try:
            # Wait for REGISTER message
            register_message = await transport.receive()
            if register_message is None:
                return

            node_id = await self._handle_registration(register_message, transport)
            if node_id is None:
                # Registration failed, close connection
                return

            # Wait for AUTHENTICATE message
            auth_message = await transport.receive()
            if auth_message is None:
                return

            authenticated = await self._handle_authentication(
                auth_message, node_id, transport
            )
            if not authenticated:
                # Authentication failed, close connection
                return

            # Store transport for heartbeat responses
            self._active_nodes[node_id] = transport

            # Handle heartbeat messages
            await self._handle_heartbeat_loop(node_id, transport)

        except Exception as e:
            # Log error and close connection
            if node_id:
                self.registry.update_node_state(node_id, NodeState.OFFLINE)
        finally:
            if node_id and node_id in self._active_nodes:
                del self._active_nodes[node_id]
            await transport.disconnect()

    async def _handle_registration(
        self,
        message: BaseMessage,
        transport: TCPTransport,
    ) -> Optional[str]:
        """Handle node registration.

        Validates the REGISTER message and registers the node in the registry.

        Args:
            message: The received BaseMessage
            transport: The transport connection

        Returns:
            The registered node_id on success, None on failure
        """
        # Validate message type
        if message.type != MessageType.REGISTER:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={"error": "Expected REGISTER message"},
            )
            await transport.send(error_response)
            return None

        # Extract node information from payload
        payload = message.payload
        if not isinstance(payload, dict):
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={"error": "Invalid payload format"},
            )
            await transport.send(error_response)
            return None

        node_id = payload.get("node_id")
        hostname = payload.get("hostname")
        connection_address = payload.get("connection_address")
        connection_port = payload.get("connection_port")

        # Validate required fields
        if not node_id or not hostname:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={"error": "Missing required fields: node_id, hostname"},
            )
            await transport.send(error_response)
            return None

        try:
            # Register node in registry
            self.registry.register_node(
                node_id=node_id,
                hostname=hostname,
                connection_address=connection_address,
                connection_port=connection_port,
            )

            # Send successful REGISTER_RESPONSE
            response = BaseMessage(
                type=MessageType.REGISTER_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={"status": "registered", "node_id": node_id},
            )
            await transport.send(response)
            return node_id

        except ValueError as e:
            # Node already registered
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={"error": str(e)},
            )
            await transport.send(error_response)
            return None

    async def _handle_authentication(
        self,
        message: BaseMessage,
        node_id: str,
        transport: TCPTransport,
    ) -> bool:
        """Handle node authentication.

        Validates the AUTHENTICATE message and authenticates the node
        using the configured Authenticator.

        Args:
            message: The received BaseMessage
            node_id: The registered node ID
            transport: The transport connection

        Returns:
            True if authentication was successful, False otherwise
        """
        # Validate message type
        if message.type != MessageType.AUTHENTICATE:
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={"status": "failed", "error": "Expected AUTHENTICATE message"},
            )
            await transport.send(error_response)
            self.registry.update_node_state(node_id, NodeState.AUTH_FAILED)
            return False

        # Extract credentials from payload
        payload = message.payload
        if not isinstance(payload, dict):
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={"status": "failed", "error": "Invalid payload format"},
            )
            await transport.send(error_response)
            self.registry.update_node_state(node_id, NodeState.AUTH_FAILED)
            return False

        credentials = payload.get("token") or payload.get("credentials")

        # Authenticate using the configured authenticator
        if self.authenticator is None:
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={"status": "failed", "error": "No authenticator configured"},
            )
            await transport.send(error_response)
            self.registry.update_node_state(node_id, NodeState.AUTH_FAILED)
            return False

        try:
            # Perform authentication
            if self.authenticator.authenticate(credentials):
                # Authentication successful
                self.registry.authenticate_node(node_id, authenticated=True)

                response = BaseMessage(
                    type=MessageType.AUTHENTICATE_RESPONSE,
                    message_id=str(uuid.uuid4()),
                    payload={"status": "authenticated", "node_id": node_id},
                )
                await transport.send(response)
                return True
            else:
                # Authentication failed
                self.registry.authenticate_node(node_id, authenticated=False)

                error_response = BaseMessage(
                    type=MessageType.AUTHENTICATE_RESPONSE,
                    message_id=str(uuid.uuid4()),
                    payload={"status": "failed", "error": "Invalid credentials"},
                )
                await transport.send(error_response)
                return False

        except AuthenticationError as e:
            # Authentication error (e.g., invalid format)
            self.registry.authenticate_node(node_id, authenticated=False)

            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={"status": "failed", "error": str(e)},
            )
            await transport.send(error_response)
            return False

    async def _handle_heartbeat_loop(
        self,
        node_id: str,
        transport: TCPTransport,
    ) -> None:
        """Handle incoming heartbeat messages from an authenticated node.

        Runs until the connection is closed or an error occurs.
        Records heartbeats and responds with HEARTBEAT_RESPONSE.

        Args:
            node_id: The authenticated node ID
            transport: The transport connection
        """
        while self._running:
            try:
                message = await transport.receive()
                if message is None:
                    # Connection closed by node
                    break

                if message.type != MessageType.HEARTBEAT:
                    # Unexpected message type during heartbeat phase
                    break

                # Validate heartbeat payload
                payload = message.payload
                if not isinstance(payload, dict):
                    break

                if payload.get("node_id") != node_id:
                    # Heartbeat from different node ID
                    break

                # Record heartbeat in registry
                self.registry.record_heartbeat(node_id)

                # Send HEARTBEAT_RESPONSE
                response = BaseMessage(
                    type=MessageType.HEARTBEAT_RESPONSE,
                    message_id=str(uuid.uuid4()),
                    payload={"status": "ok", "node_id": node_id},
                )
                await transport.send(response)

            except Exception as e:
                # Connection error or transport failure, exit heartbeat loop
                break

    async def _run_offline_detection(self) -> None:
        """Run the offline node detection loop.

        Periodically scans the registry for nodes that have exceeded
        the heartbeat timeout and marks them as OFFLINE.
        Runs at half the heartbeat_timeout_seconds interval.
        """
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_timeout_seconds / 2)

                if not self._running:
                    break

                # Detect offline nodes
                offline_nodes = self.registry.detect_offline_nodes(
                    self.heartbeat_timeout_seconds
                )

                # Mark detected offline nodes
                for node_info in offline_nodes:
                    self.registry.mark_offline(node_info.node_id)

            except asyncio.CancelledError:
                # Task cancelled during stop()
                break
            except Exception as e:
                # Log error and continue detection loop
                pass
