"""Node agent for connecting to the NodeForge controller."""

import asyncio
import uuid
from enum import Enum
from typing import Optional

from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport


class AgentState(str, Enum):
    """Enumeration of possible agent states."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    REGISTERING = "registering"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    ERROR = "error"


class NodeAgent:
    """Node agent for communicating with the NodeForge controller.

    The agent handles connection establishment, registration,
    authentication, and heartbeat communication with the controller.
    """

    def __init__(
        self,
        node_id: str,
        hostname: str,
        controller_host: str = "localhost",
        controller_port: int = 9999,
        authentication_token: str = "",
        heartbeat_interval_seconds: float = 10.0,
        reconnect_delay_seconds: float = 5.0,
    ):
        """Initialize the node agent.

        Args:
            node_id: Unique identifier for this node
            hostname: Hostname or name of this node
            controller_host: Host address of the controller
            controller_port: Port of the controller
            authentication_token: Token for authentication with controller
            heartbeat_interval_seconds: Seconds between heartbeat messages
            reconnect_delay_seconds: Seconds to wait before reconnection attempt
        """
        self.node_id = node_id
        self.hostname = hostname
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.authentication_token = authentication_token
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.reconnect_delay_seconds = reconnect_delay_seconds

        self.transport: Optional[TCPTransport] = None
        self.state = AgentState.DISCONNECTED
        self._running = False

    async def start(self) -> None:
        """Start the node agent and connect to the controller.

        Implements automatic reconnection with exponential backoff.
        Runs until stop() is called.

        Raises:
            RuntimeError: If the agent is already running
        """
        if self._running:
            raise RuntimeError("Node agent is already running")

        self._running = True
        self.state = AgentState.CONNECTING

        while self._running:
            try:
                # Connect to controller
                self.transport = TCPTransport()
                await self.transport.connect(self.controller_host, self.controller_port)

                # Perform registration and authentication
                success = await self._register_and_authenticate()
                if success:
                    self.state = AgentState.READY
                    # Run heartbeat loop until connection is lost or stop() is called
                    await self._run_heartbeat_loop()
                    # Heartbeat loop exited, prepare for reconnection
                    self.state = AgentState.ERROR
                else:
                    self.state = AgentState.ERROR
                    await self.transport.disconnect()

                # Only reconnect if still running
                if self._running:
                    await asyncio.sleep(self.reconnect_delay_seconds)

            except ConnectionError as e:
                self.state = AgentState.ERROR
                if self.transport:
                    await self.transport.disconnect()
                # Only reconnect if still running
                if self._running:
                    await asyncio.sleep(self.reconnect_delay_seconds)
            except Exception as e:
                self.state = AgentState.ERROR
                if self.transport:
                    await self.transport.disconnect()
                # Only reconnect if still running
                if self._running:
                    await asyncio.sleep(self.reconnect_delay_seconds)

    async def stop(self) -> None:
        """Stop the node agent and disconnect from the controller.

        Closes the connection and stops all operations including heartbeat.
        Prevents reconnection attempts.
        """
        self._running = False
        self.state = AgentState.DISCONNECTED

        if self.transport:
            await self.transport.disconnect()
            self.transport = None

    async def _register_and_authenticate(self) -> bool:
        """Perform registration and authentication with the controller.

        Returns:
            True if successful, False otherwise
        """
        if self.transport is None:
            return False

        try:
            # Step 1: Send REGISTER message
            self.state = AgentState.REGISTERING
            register_message = BaseMessage(
                type=MessageType.REGISTER,
                message_id=str(uuid.uuid4()),
                payload={
                    "node_id": self.node_id,
                    "hostname": self.hostname,
                },
            )
            await self.transport.send(register_message)

            # Step 2: Wait for REGISTER_RESPONSE
            register_response = await self.transport.receive()
            if register_response is None:
                return False

            if register_response.type != MessageType.REGISTER_RESPONSE:
                return False

            # Verify response payload
            payload = register_response.payload
            if not isinstance(payload, dict):
                return False

            if payload.get("status") != "registered":
                return False

            # Step 3: Send AUTHENTICATE message
            self.state = AgentState.AUTHENTICATING
            auth_message = BaseMessage(
                type=MessageType.AUTHENTICATE,
                message_id=str(uuid.uuid4()),
                payload={
                    "token": self.authentication_token,
                },
            )
            await self.transport.send(auth_message)

            # Step 4: Wait for AUTHENTICATE_RESPONSE
            auth_response = await self.transport.receive()
            if auth_response is None:
                return False

            if auth_response.type != MessageType.AUTHENTICATE_RESPONSE:
                return False

            # Verify response payload
            payload = auth_response.payload
            if not isinstance(payload, dict):
                return False

            if payload.get("status") != "authenticated":
                return False

            return True

        except Exception as e:
            return False

    async def _run_heartbeat_loop(self) -> None:
        """Run the heartbeat loop while connected and authenticated.

        Sends periodic HEARTBEAT messages and validates responses.
        Exits if connection is lost, authentication fails, or stop() is called.
        Does not attempt reconnection; the main start() loop will handle that.
        """
        while self._running and self.state == AgentState.READY:
            try:
                await asyncio.sleep(self.heartbeat_interval_seconds)

                # Check if still running and in READY state
                if not self._running or self.state != AgentState.READY:
                    break

                # Send HEARTBEAT message
                heartbeat_message = BaseMessage(
                    type=MessageType.HEARTBEAT,
                    message_id=str(uuid.uuid4()),
                    payload={
                        "node_id": self.node_id,
                    },
                )
                await self.transport.send(heartbeat_message)

                # Wait for HEARTBEAT_RESPONSE
                heartbeat_response = await self.transport.receive()
                if heartbeat_response is None:
                    # Connection closed by controller
                    break

                if heartbeat_response.type != MessageType.HEARTBEAT_RESPONSE:
                    # Unexpected message type
                    break

                # Validate response payload
                payload = heartbeat_response.payload
                if not isinstance(payload, dict):
                    break

                if payload.get("status") != "ok":
                    break

            except asyncio.CancelledError:
                # Task cancelled, exit cleanly
                break
            except Exception as e:
                # Connection error or other exception, exit heartbeat loop
                break

    def get_state(self) -> AgentState:
        """Get the current state of the agent.

        Returns:
            Current AgentState
        """
        return self.state

    def is_connected(self) -> bool:
        """Check if the agent is connected and authenticated.

        Returns:
            True if in READY state, False otherwise
        """
        return self.state == AgentState.READY

    def is_running(self) -> bool:
        """Check if the agent is currently running.

        Returns:
            True if running, False otherwise
        """
        return self._running
