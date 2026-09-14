"""Controller for the NodeForge distributed system."""

import asyncio
import uuid
from typing import Dict, Optional

from freemesh.controller.node_registry import NodeRegistry, NodeState
from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport
from freemesh.security.auth import Authenticator, AuthenticationError


class Controller:
    """Central controller for managing NodeForge nodes."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9999,
        heartbeat_timeout_seconds: float = 30.0,
        authenticator: Optional[Authenticator] = None,
    ):
        self.host = host
        self.port = port
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.authenticator = authenticator

        self.registry = NodeRegistry()
        self.server: Optional[asyncio.Server] = None
        self._running = False
        self._offline_detection_task: Optional[asyncio.Task] = None

        self._active_nodes: Dict[str, TCPTransport] = {}

        self._service_responses: Dict[str, BaseMessage] = {}
        self._service_response_events: Dict[str, asyncio.Event] = {}

    async def start(self) -> None:
        """Start the controller."""

        if self.server is not None:
            raise RuntimeError("Controller is already running")

        self._running = True

        self.server = await asyncio.start_server(
            self._handle_client_connection,
            self.host,
            self.port,
        )

        self._offline_detection_task = asyncio.create_task(
            self._run_offline_detection()
        )

        async with self.server:
            await self.server.serve_forever()

    async def stop(self) -> None:
        """Stop the controller and close all connections."""

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

        for transport in list(self._active_nodes.values()):
            try:
                await transport.disconnect()
            except Exception:
                pass

        self._active_nodes.clear()

        for event in self._service_response_events.values():
            event.set()

        self._service_response_events.clear()
        self._service_responses.clear()

    async def _handle_client_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle an incoming node connection."""

        transport = TCPTransport()
        transport.reader = reader
        transport.writer = writer

        node_id: Optional[str] = None

        try:
            register_message = await transport.receive()

            if register_message is None:
                return

            node_id = await self._handle_registration(
                register_message,
                transport,
            )

            if node_id is None:
                return

            auth_message = await transport.receive()

            if auth_message is None:
                return

            authenticated = await self._handle_authentication(
                auth_message,
                node_id,
                transport,
            )

            if not authenticated:
                return

            self._active_nodes[node_id] = transport

            await self._handle_node_message_loop(
                node_id,
                transport,
            )

        except Exception:
            if node_id:
                try:
                    self.registry.update_node_state(
                        node_id,
                        NodeState.OFFLINE,
                    )
                except KeyError:
                    pass

        finally:
            if node_id:
                self._active_nodes.pop(node_id, None)

            await transport.disconnect()

    async def _handle_registration(
        self,
        message: BaseMessage,
        transport: TCPTransport,
    ) -> Optional[str]:
        """Handle node registration."""

        if message.type != MessageType.REGISTER:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": "Expected REGISTER message",
                },
            )

            await transport.send(error_response)
            return None

        payload = message.payload

        if not isinstance(payload, dict):
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": "Invalid payload format",
                },
            )

            await transport.send(error_response)
            return None

        node_id = payload.get("node_id")
        hostname = payload.get("hostname")
        connection_address = payload.get("connection_address")
        connection_port = payload.get("connection_port")

        if not node_id or not hostname:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": "Missing required fields: node_id, hostname",
                },
            )

            await transport.send(error_response)
            return None

        try:
            self.registry.register_node(
                node_id=node_id,
                hostname=hostname,
                connection_address=connection_address,
                connection_port=connection_port,
            )

            response = BaseMessage(
                type=MessageType.REGISTER_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "registered",
                    "node_id": node_id,
                },
            )

            await transport.send(response)

            return node_id

        except ValueError as exc:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": str(exc),
                },
            )

            await transport.send(error_response)

            return None

    async def _handle_authentication(
        self,
        message: BaseMessage,
        node_id: str,
        transport: TCPTransport,
    ) -> bool:
        """Handle node authentication."""

        if message.type != MessageType.AUTHENTICATE:
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "Expected AUTHENTICATE message",
                },
            )

            await transport.send(error_response)

            self.registry.update_node_state(
                node_id,
                NodeState.AUTH_FAILED,
            )

            return False

        payload = message.payload

        if not isinstance(payload, dict):
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "Invalid payload format",
                },
            )

            await transport.send(error_response)

            self.registry.update_node_state(
                node_id,
                NodeState.AUTH_FAILED,
            )

            return False

        credentials = payload.get("token") or payload.get("credentials")

        if self.authenticator is None:
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "No authenticator configured",
                },
            )

            await transport.send(error_response)

            self.registry.update_node_state(
                node_id,
                NodeState.AUTH_FAILED,
            )

            return False

        try:
            authenticated = self.authenticator.authenticate(
                credentials
            )

            if authenticated:
                self.registry.authenticate_node(
                    node_id,
                    authenticated=True,
                )

                response = BaseMessage(
                    type=MessageType.AUTHENTICATE_RESPONSE,
                    message_id=str(uuid.uuid4()),
                    payload={
                        "status": "authenticated",
                        "node_id": node_id,
                    },
                )

                await transport.send(response)

                return True

            self.registry.authenticate_node(
                node_id,
                authenticated=False,
            )

            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "Invalid credentials",
                },
            )

            await transport.send(error_response)

            return False

        except AuthenticationError as exc:
            self.registry.authenticate_node(
                node_id,
                authenticated=False,
            )

            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": str(exc),
                },
            )

            await transport.send(error_response)

            return False

    async def _handle_node_message_loop(
        self,
        node_id: str,
        transport: TCPTransport,
    ) -> None:
        """Receive and dispatch all messages from an authenticated node."""

        while self._running:
            try:
                message = await transport.receive()

                if message is None:
                    break

                if message.type == MessageType.HEARTBEAT:
                    await self._handle_heartbeat(
                        node_id,
                        message,
                        transport,
                    )

                elif message.type in (
                    MessageType.SERVICE_START_RESPONSE,
                    MessageType.SERVICE_STOP_RESPONSE,
                    MessageType.SERVICE_STATUS_RESPONSE,
                ):
                    self._store_service_response(message)

                elif message.type == MessageType.ERROR:
                    self._store_service_response(message)

                else:
                    continue

            except asyncio.CancelledError:
                break

            except Exception:
                break

    async def _handle_heartbeat(
        self,
        node_id: str,
        message: BaseMessage,
        transport: TCPTransport,
    ) -> None:
        """Handle a heartbeat message."""

        payload = message.payload

        if not isinstance(payload, dict):
            return

        if payload.get("node_id") != node_id:
            return

        self.registry.record_heartbeat(node_id)

        response = BaseMessage(
            type=MessageType.HEARTBEAT_RESPONSE,
            message_id=message.message_id,
            payload={
                "status": "ok",
                "node_id": node_id,
            },
        )

        await transport.send(response)

    def _store_service_response(
        self,
        message: BaseMessage,
    ) -> None:
        """Store a service response and wake its waiter."""

        request_id = message.payload.get("request_id")

        if not request_id:
            request_id = message.message_id

        self._service_responses[request_id] = message

        event = self._service_response_events.get(request_id)

        if event:
            event.set()

    async def start_service(
        self,
        node_id: str,
        service_id: str,
        command: str,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Request a node to start a service."""

        transport = self._active_nodes.get(node_id)

        if transport is None:
            raise RuntimeError(
                f"Node {node_id} is not connected"
            )

        request_id = str(uuid.uuid4())

        event = asyncio.Event()

        self._service_response_events[request_id] = event

        message = BaseMessage(
            type=MessageType.SERVICE_START,
            message_id=request_id,
            payload={
                "service_id": service_id,
                "command": command,
                "request_id": request_id,
            },
        )

        try:
            await transport.send(message)

            await asyncio.wait_for(
                event.wait(),
                timeout=timeout_seconds,
            )

            response = self._service_responses.get(request_id)

            if response is None:
                raise RuntimeError(
                    "Service start response was not received"
                )

            return response

        finally:
            self._service_response_events.pop(
                request_id,
                None,
            )
            self._service_responses.pop(
                request_id,
                None,
            )

    async def stop_service(
        self,
        node_id: str,
        service_id: str,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Request a node to stop a service."""

        transport = self._active_nodes.get(node_id)

        if transport is None:
            raise RuntimeError(
                f"Node {node_id} is not connected"
            )

        request_id = str(uuid.uuid4())

        event = asyncio.Event()

        self._service_response_events[request_id] = event

        message = BaseMessage(
            type=MessageType.SERVICE_STOP,
            message_id=request_id,
            payload={
                "service_id": service_id,
                "request_id": request_id,
            },
        )

        try:
            await transport.send(message)

            await asyncio.wait_for(
                event.wait(),
                timeout=timeout_seconds,
            )

            response = self._service_responses.get(request_id)

            if response is None:
                raise RuntimeError(
                    "Service stop response was not received"
                )

            return response

        finally:
            self._service_response_events.pop(
                request_id,
                None,
            )
            self._service_responses.pop(
                request_id,
                None,
            )

    async def status_service(
        self,
        node_id: str,
        service_id: str,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Request the current status of a service."""

        transport = self._active_nodes.get(node_id)

        if transport is None:
            raise RuntimeError(
                f"Node {node_id} is not connected"
            )

        request_id = str(uuid.uuid4())

        event = asyncio.Event()

        self._service_response_events[request_id] = event

        message = BaseMessage(
            type=MessageType.SERVICE_STATUS,
            message_id=request_id,
            payload={
                "service_id": service_id,
                "request_id": request_id,
            },
        )

        try:
            await transport.send(message)

            await asyncio.wait_for(
                event.wait(),
                timeout=timeout_seconds,
            )

            response = self._service_responses.get(request_id)

            if response is None:
                raise RuntimeError(
                    "Service status response was not received"
                )

            return response

        finally:
            self._service_response_events.pop(
                request_id,
                None,
            )
            self._service_responses.pop(
                request_id,
                None,
            )

    async def _run_offline_detection(self) -> None:
        """Run the offline node detection loop."""

        while self._running:
            try:
                await asyncio.sleep(
                    self.heartbeat_timeout_seconds / 2
                )

                if not self._running:
                    break

                offline_nodes = self.registry.detect_offline_nodes(
                    self.heartbeat_timeout_seconds
                )

                for node_info in offline_nodes:
                    self.registry.mark_offline(
                        node_info.node_id
                    )

            except asyncio.CancelledError:
                break

            except Exception:
                pass