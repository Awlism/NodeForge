"""Transport layer for NodeForge protocol communication."""

import asyncio
import json
import struct
from abc import ABC, abstractmethod
from typing import Optional

from .messages import BaseMessage


class Transport(ABC):
    """Abstract base class for transport implementations."""

    @abstractmethod
    async def connect(self, host: str, port: int) -> None:
        """Connect to a remote endpoint."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the remote endpoint."""
        pass

    @abstractmethod
    async def send(self, message: BaseMessage) -> None:
        """Send a message to the remote endpoint."""
        pass

    @abstractmethod
    async def receive(self) -> Optional[BaseMessage]:
        """Receive a message from the remote endpoint."""
        pass

    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if transport is currently connected."""
        pass


class TCPTransport(Transport):
    """TCP transport with length-prefixed JSON messages."""

    def __init__(self, host: str = "localhost", port: int = 9999):
        """Initialize TCP transport.

        Args:
            host: Host to connect to or bind on
            port: Port to connect to or bind on
        """
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def connect(self, host: str, port: int) -> None:
        """Connect to a remote TCP endpoint.

        Args:
            host: Remote host to connect to
            port: Remote port to connect to

        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10.0
            )
        except asyncio.TimeoutError as e:
            raise ConnectionError(f"Connection timeout to {host}:{port}") from e
        except OSError as e:
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from the remote endpoint."""
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader = None
        self.writer = None

    async def send(self, message: BaseMessage) -> None:
        """Send a message using length-prefixed JSON framing.

        Args:
            message: Message to send

        Raises:
            ConnectionError: If not connected or send fails
        """
        if not self.writer:
            raise ConnectionError("Transport is not connected")

        try:
            # Serialize message to JSON
            message_json = message.model_dump_json()
            message_bytes = message_json.encode("utf-8")

            # Create length-prefixed frame: 4-byte big-endian length + message
            frame_length = struct.pack(">I", len(message_bytes))
            frame = frame_length + message_bytes

            # Send frame
            self.writer.write(frame)
            await self.writer.drain()
        except (OSError, BrokenPipeError) as e:
            raise ConnectionError(f"Failed to send message: {e}") from e

    async def receive(self) -> Optional[BaseMessage]:
        """Receive a message using length-prefixed JSON framing.

        Args:
            Returns:
                Received BaseMessage or None if disconnected

        Raises:
            ValueError: If received message is invalid
            ConnectionError: If receive fails
        """
        if not self.reader:
            raise ConnectionError("Transport is not connected")

        try:
            # Read 4-byte length prefix
            length_bytes = await self.reader.readexactly(4)
            if not length_bytes:
                return None

            message_length = struct.unpack(">I", length_bytes)[0]

            # Validate message length
            if message_length > 1024 * 1024:  # 1MB max
                raise ValueError(f"Message length {message_length} exceeds maximum")

            # Read message body
            message_bytes = await self.reader.readexactly(message_length)
            if not message_bytes:
                return None

            # Deserialize JSON to BaseMessage
            message_json = message_bytes.decode("utf-8")
            message_dict = json.loads(message_json)
            message = BaseMessage(**message_dict)

            return message
        except asyncio.IncompleteReadError:
            # Connection closed by remote
            return None
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in message: {e}") from e
        except (OSError, asyncio.TimeoutError) as e:
            raise ConnectionError(f"Failed to receive message: {e}") from e

    async def is_connected(self) -> bool:
        """Check if transport is connected.

        Returns:
            True if connected, False otherwise
        """
        return self.writer is not None and self.reader is not None
