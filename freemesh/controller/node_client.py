"""Controller-side client for NodeForge nodes."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from freemesh.protocol.messages import (
    BaseMessage,
    MessageType,
)
from freemesh.protocol.transport import (
    TCPTransport,
)


class NodeClient:
    """Client used by the Controller to communicate with a Node."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        timeout: float = 10.0,
    ) -> None:
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
                host,
                str,
            )
            or not host.strip()
        ):
            raise ValueError(
                "host is required"
            )

        if (
            not isinstance(
                port,
                int,
            )
            or isinstance(
                port,
                bool,
            )
            or not 1 <= port <= 65535
        ):
            raise ValueError(
                "port must be between 1 and 65535"
            )

        if (
            not isinstance(
                timeout,
                (int, float),
            )
            or isinstance(
                timeout,
                bool,
            )
            or timeout <= 0
        ):
            raise ValueError(
                "timeout must be greater than 0"
            )

        self.node_id = node_id
        self.host = host
        self.port = port
        self.timeout = float(timeout)

        self.transport: Optional[
            TCPTransport
        ] = None

        self.connected = False
        self.authenticated = False

        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to the remote Node."""

        async with self._lock:
            if (
                self.connected
                and self.transport is not None
            ):
                return

            transport = TCPTransport(
                host=self.host,
                port=self.port,
                connect_timeout_seconds=(
                    self.timeout
                ),
            )

            try:
                await asyncio.wait_for(
                    transport.connect(
                        self.host,
                        self.port,
                    ),
                    timeout=self.timeout,
                )

            except asyncio.CancelledError:
                await transport.disconnect()
                raise

            except Exception:
                await transport.disconnect()
                raise

            self.transport = transport
            self.connected = True
            self.authenticated = False

    async def disconnect(self) -> None:
        """Disconnect from the remote Node."""

        async with self._lock:
            transport = self.transport

            self.transport = None
            self.connected = False
            self.authenticated = False

            if transport is None:
                return

            await transport.disconnect()

    async def send(
        self,
        message: BaseMessage,
    ) -> BaseMessage:
        """Send a message and wait for its response."""

        if not isinstance(
            message,
            BaseMessage,
        ):
            raise TypeError(
                "message must be a BaseMessage instance"
            )

        async with self._lock:
            transport = self.transport

            if (
                not self.connected
                or transport is None
            ):
                raise ConnectionError(
                    f"Node {self.node_id} "
                    "is not connected"
                )

            try:
                await asyncio.wait_for(
                    transport.send(message),
                    timeout=self.timeout,
                )

                response = await asyncio.wait_for(
                    transport.receive(),
                    timeout=self.timeout,
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                self.connected = False
                self.authenticated = False

                try:
                    await transport.disconnect()
                except Exception:
                    pass

                self.transport = None

                raise

            if response is None:
                self.connected = False
                self.authenticated = False

                try:
                    await transport.disconnect()
                except Exception:
                    pass

                self.transport = None

                raise ConnectionError(
                    f"Node {self.node_id} disconnected"
                )

            return response

    async def authenticate(
        self,
        token: str,
    ) -> BaseMessage:
        """Authenticate with the remote Node."""

        if (
            not isinstance(
                token,
                str,
            )
            or not token
        ):
            raise ValueError(
                "token is required"
            )

        message = BaseMessage(
            type=MessageType.AUTHENTICATE,
            payload={
                "node_id": self.node_id,
                "token": token,
            },
        )

        response = await self.send(
            message
        )

        if (
            response.type
            == MessageType.AUTHENTICATE_RESPONSE
        ):
            self.authenticated = bool(
                response.payload.get(
                    "authenticated",
                    response.payload.get(
                        "status"
                    )
                        == "authenticated",
                )
            )

        return response

    async def start_service(
        self,
        service_id: str,
        command: str,
        requirements: Optional[
            dict[str, Any]
        ] = None,
    ) -> BaseMessage:
        """Start a service on the remote Node."""

        if (
            not isinstance(
                service_id,
                str,
            )
            or not service_id.strip()
        ):
            raise ValueError(
                "service_id is required"
            )

        if (
            not isinstance(
                command,
                str,
            )
            or not command.strip()
        ):
            raise ValueError(
                "command is required"
            )

        if (
            requirements is not None
            and not isinstance(
                requirements,
                dict,
            )
        ):
            raise TypeError(
                "requirements must be a dictionary"
            )

        payload: dict[str, Any] = {
            "node_id": self.node_id,
            "service_id": service_id,
            "command": command,
        }

        if requirements is not None:
            payload[
                "requirements"
            ] = requirements

        message = BaseMessage(
            type=MessageType.SERVICE_START,
            payload=payload,
        )

        return await self.send(
            message
        )

    async def stop_service(
        self,
        service_id: str,
    ) -> BaseMessage:
        """Stop a service on the remote Node."""

        if (
            not isinstance(
                service_id,
                str,
            )
            or not service_id.strip()
        ):
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

        return await self.send(
            message
        )

    async def get_service_status(
        self,
        service_id: str,
    ) -> BaseMessage:
        """Get service status."""

        if (
            not isinstance(
                service_id,
                str,
            )
            or not service_id.strip()
        ):
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

        return await self.send(
            message
        )

    async def send_heartbeat(
        self,
    ) -> BaseMessage:
        """Send a heartbeat."""

        message = BaseMessage(
            type=MessageType.HEARTBEAT,
            payload={
                "node_id": self.node_id,
            },
        )

        return await self.send(
            message
        )

    async def request_resources(
        self,
    ) -> BaseMessage:
        """Request current resource information."""

        message = BaseMessage(
            type=MessageType.RESOURCE_REPORT,
            payload={
                "node_id": self.node_id,
            },
        )

        return await self.send(
            message
        )

    async def send_command(
        self,
        message_type: MessageType,
        payload: Optional[
            dict[str, Any]
        ] = None,
    ) -> BaseMessage:
        """Send a generic protocol command."""

        if not isinstance(
            message_type,
            MessageType,
        ):
            raise TypeError(
                "message_type must be a MessageType"
            )

        if (
            payload is not None
            and not isinstance(
                payload,
                dict,
            )
        ):
            raise TypeError(
                "payload must be a dictionary"
            )

        message = BaseMessage(
            type=message_type,
            payload={
                "node_id": self.node_id,
                **(payload or {}),
            },
        )

        return await self.send(
            message
        )

    async def __aenter__(
        self,
    ) -> "NodeClient":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> None:
        await self.disconnect()