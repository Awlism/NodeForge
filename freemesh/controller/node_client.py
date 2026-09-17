"""Controller-side client for communicating with NodeForge nodes."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.transport.tcp import TCPTransport


class NodeClient:
    """Maintain a Controller-side connection to a NodeForge node."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        timeout: float = 10.0,
    ) -> None:
        if not node_id:
            raise ValueError("node_id is required")

        if not host:
            raise ValueError("host is required")

        if port <= 0 or port > 65535:
            raise ValueError(
                "port must be between 1 and 65535"
            )

        if timeout <= 0:
            raise ValueError(
                "timeout must be greater than 0"
            )

        self.node_id = node_id
        self.host = host
        self.port = port
        self.timeout = timeout

        self.transport: Optional[TCPTransport] = None

        self.connected = False
        self.authenticated = False

        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to the NodeForge node."""

        async with self._lock:
            if self.connected and self.transport is not None:
                return

            transport = TCPTransport()

            await asyncio.wait_for(
                transport.connect(
                    self.host,
                    self.port,
                ),
                timeout=self.timeout,
            )

            self.transport = transport
            self.connected = True
            self.authenticated = False

    async def disconnect(self) -> None:
        """Disconnect from the node."""

        async with self._lock:
            transport = self.transport

            self.transport = None
            self.connected = False
            self.authenticated = False

            if transport is None:
                return

            disconnect = getattr(
                transport,
                "disconnect",
                None,
            )

            if disconnect is not None:
                result = disconnect()

                if asyncio.iscoroutine(result):
                    await result

    async def send(
        self,
        message: BaseMessage,
    ) -> BaseMessage:
        """Send a message and wait for the response."""

        if not isinstance(message, BaseMessage):
            raise TypeError(
                "message must be a BaseMessage instance"
            )

        transport = self.transport

        if not self.connected or transport is None:
            raise ConnectionError(
                f"Node {self.node_id} is not connected"
            )

        send_message = getattr(
            transport,
            "send",
            None,
        )

        if send_message is None:
            send_message = getattr(
                transport,
                "send_message",
                None,
            )

        if send_message is None:
            raise RuntimeError(
                "TCPTransport does not provide a send method"
            )

        result = send_message(message)

        if asyncio.iscoroutine(result):
            result = await asyncio.wait_for(
                result,
                timeout=self.timeout,
            )

        if not isinstance(result, BaseMessage):
            raise TypeError(
                "transport response must be a BaseMessage instance"
            )

        return result

    async def authenticate(
        self,
        token: str,
    ) -> BaseMessage:
        """Authenticate the Controller connection with the node."""

        if not token:
            raise ValueError("token is required")

        message = BaseMessage(
            type=MessageType.AUTHENTICATE,
            payload={
                "node_id": self.node_id,
                "token": token,
            },
        )

        response = await self.send(message)

        if response.type == MessageType.AUTHENTICATE_RESPONSE:
            authenticated = response.payload.get(
                "authenticated",
                True,
            )

            self.authenticated = bool(authenticated)

        return response

    async def register_service(
        self,
        service_id: str,
        command: str,
        requirements: Optional[dict[str, Any]] = None,
    ) -> BaseMessage:
        """Request service registration/start on the node."""

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        if not command:
            raise ValueError(
                "command is required"
            )

        payload: dict[str, Any] = {
            "node_id": self.node_id,
            "service_id": service_id,
            "command": command,
        }

        if requirements is not None:
            payload["requirements"] = requirements

        message = BaseMessage(
            type=MessageType.SERVICE_START,
            payload=payload,
        )

        return await self.send(message)

    async def start_service(
        self,
        service_id: str,
        command: str,
        requirements: Optional[dict[str, Any]] = None,
    ) -> BaseMessage:
        """Start a service on the node."""

        return await self.register_service(
            service_id=service_id,
            command=command,
            requirements=requirements,
        )

    async def stop_service(
        self,
        service_id: str,
    ) -> BaseMessage:
        """Stop a service on the node."""

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        message = BaseMessage(
            type=MessageType.SERVICE_STOP,
            payload={
                "node_id": self.node_id,
                "service_id": service_id,
            },
        )

        return await self.send(message)

    async def get_service_status(
        self,
        service_id: str,
    ) -> BaseMessage:
        """Request the current status of a service."""

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        message = BaseMessage(
            type=MessageType.SERVICE_STATUS,
            payload={
                "node_id": self.node_id,
                "service_id": service_id,
            },
        )

        return await self.send(message)

    async def send_heartbeat(self) -> BaseMessage:
        """Send a heartbeat request to the node."""

        message = BaseMessage(
            type=MessageType.HEARTBEAT,
            payload={
                "node_id": self.node_id,
            },
        )

        return await self.send(message)

    async def request_resources(self) -> BaseMessage:
        """Request the node's current resource information."""

        message = BaseMessage(
            type=MessageType.RESOURCE_REPORT,
            payload={
                "node_id": self.node_id,
            },
        )

        return await self.send(message)

    async def send_command(
        self,
        message_type: MessageType,
        payload: Optional[dict[str, Any]] = None,
    ) -> BaseMessage:
        """Send a generic protocol command to the node."""

        if not isinstance(
            message_type,
            MessageType,
        ):
            raise TypeError(
                "message_type must be a MessageType"
            )

        message = BaseMessage(
            type=message_type,
            payload={
                "node_id": self.node_id,
                **(payload or {}),
            },
        )

        return await self.send(message)

    async def __aenter__(self) -> "NodeClient":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> None:
        await self.disconnect()