"""Transport layer for NodeForge protocol communication."""

from __future__ import annotations

import asyncio
import json
import struct
from abc import ABC, abstractmethod
from typing import Optional

from .messages import BaseMessage


class Transport(ABC):
    """Abstract transport interface."""

    @abstractmethod
    async def connect(
        self,
        host: str,
        port: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def disconnect(
        self,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send(
        self,
        message: BaseMessage,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def receive(
        self,
    ) -> Optional[BaseMessage]:
        raise NotImplementedError

    @abstractmethod
    async def is_connected(
        self,
    ) -> bool:
        raise NotImplementedError


class TCPTransport(Transport):
    """TCP transport with bounded framed JSON messages."""

    DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
    DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024
    DEFAULT_SEND_TIMEOUT_SECONDS = 10.0
    DEFAULT_RECEIVE_TIMEOUT_SECONDS = 30.0

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9999,
        *,
        connect_timeout_seconds: float = (
            DEFAULT_CONNECT_TIMEOUT_SECONDS
        ),
        max_message_size: int = (
            DEFAULT_MAX_MESSAGE_SIZE
        ),
        send_timeout_seconds: float = (
            DEFAULT_SEND_TIMEOUT_SECONDS
        ),
        receive_timeout_seconds: float = (
            DEFAULT_RECEIVE_TIMEOUT_SECONDS
        ),
    ) -> None:
        if connect_timeout_seconds <= 0:
            raise ValueError(
                "connect_timeout_seconds "
                "must be positive"
            )

        if max_message_size <= 0:
            raise ValueError(
                "max_message_size "
                "must be positive"
            )

        if send_timeout_seconds <= 0:
            raise ValueError(
                "send_timeout_seconds "
                "must be positive"
            )

        if receive_timeout_seconds <= 0:
            raise ValueError(
                "receive_timeout_seconds "
                "must be positive"
            )

        self.host = host
        self.port = port

        self.connect_timeout_seconds = (
            connect_timeout_seconds
        )

        self.max_message_size = (
            max_message_size
        )

        self.send_timeout_seconds = (
            send_timeout_seconds
        )

        self.receive_timeout_seconds = (
            receive_timeout_seconds
        )

        self.reader: Optional[
            asyncio.StreamReader
        ] = None

        self.writer: Optional[
            asyncio.StreamWriter
        ] = None

        self._io_lock = asyncio.Lock()

    async def connect(
        self,
        host: str,
        port: int,
    ) -> None:
        """Connect to a remote TCP endpoint."""

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

        async with self._io_lock:
            if (
                self.writer is not None
                and not self.writer.is_closing()
            ):
                return

            self.reader = None
            self.writer = None

            try:
                reader, writer = (
                    await asyncio.wait_for(
                        asyncio.open_connection(
                            host,
                            port,
                        ),
                        timeout=(
                            self.connect_timeout_seconds
                        ),
                    )
                )

            except asyncio.TimeoutError as exc:
                raise ConnectionError(
                    f"Connection timeout to "
                    f"{host}:{port}"
                ) from exc

            except OSError as exc:
                raise ConnectionError(
                    f"Failed to connect to "
                    f"{host}:{port}: {exc}"
                ) from exc

            self.reader = reader
            self.writer = writer

    async def disconnect(
        self,
    ) -> None:
        """Disconnect cleanly."""

        async with self._io_lock:
            writer = self.writer

            self.reader = None
            self.writer = None

            if writer is None:
                return

            writer.close()

            try:
                await writer.wait_closed()
            except (
                asyncio.CancelledError,
            ):
                raise
            except Exception:
                pass

    async def send(
        self,
        message: BaseMessage,
    ) -> None:
        """Send one bounded length-prefixed JSON frame."""

        if not isinstance(
            message,
            BaseMessage,
        ):
            raise TypeError(
                "message must be a BaseMessage"
            )

        async with self._io_lock:
            writer = self.writer

            if writer is None:
                raise ConnectionError(
                    "Transport is not connected"
                )

            if writer.is_closing():
                raise ConnectionError(
                    "Transport writer is closed"
                )

            try:
                message_json = (
                    message.model_dump_json()
                )

                message_bytes = (
                    message_json.encode(
                        "utf-8"
                    )
                )

                message_length = len(
                    message_bytes
                )

                if (
                    message_length
                    <= 0
                ):
                    raise ValueError(
                        "Message cannot be empty"
                    )

                if (
                    message_length
                    > self.max_message_size
                ):
                    raise ValueError(
                        "Message exceeds maximum "
                        f"size of "
                        f"{self.max_message_size} bytes"
                    )

                frame = (
                    struct.pack(
                        ">I",
                        message_length,
                    )
                    + message_bytes
                )

                writer.write(frame)

                await asyncio.wait_for(
                    writer.drain(),
                    timeout=(
                        self.send_timeout_seconds
                    ),
                )

            except asyncio.TimeoutError as exc:
                raise ConnectionError(
                    "Timed out while sending message"
                ) from exc

            except ValueError:
                raise

            except (
                OSError,
                BrokenPipeError,
            ) as exc:
                raise ConnectionError(
                    f"Failed to send message: {exc}"
                ) from exc

    async def receive(
        self,
    ) -> Optional[BaseMessage]:
        """Receive one bounded length-prefixed JSON frame."""

        async with self._io_lock:
            reader = self.reader

            if reader is None:
                raise ConnectionError(
                    "Transport is not connected"
                )

            try:
                length_bytes = (
                    await asyncio.wait_for(
                        reader.readexactly(4),
                        timeout=(
                            self.receive_timeout_seconds
                        ),
                    )
                )

                message_length = struct.unpack(
                    ">I",
                    length_bytes,
                )[0]

                if message_length <= 0:
                    raise ValueError(
                        "Message length "
                        "must be positive"
                    )

                if (
                    message_length
                    > self.max_message_size
                ):
                    raise ValueError(
                        f"Message length "
                        f"{message_length} exceeds "
                        f"maximum of "
                        f"{self.max_message_size}"
                    )

                message_bytes = (
                    await asyncio.wait_for(
                        reader.readexactly(
                            message_length
                        ),
                        timeout=(
                            self.receive_timeout_seconds
                        ),
                    )
                )

                try:
                    message_json = (
                        message_bytes.decode(
                            "utf-8"
                        )
                    )

                    message_dict = json.loads(
                        message_json
                    )

                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                ) as exc:
                    raise ValueError(
                        "Invalid JSON message"
                    ) from exc

                if not isinstance(
                    message_dict,
                    dict,
                ):
                    raise ValueError(
                        "Message payload must "
                        "be a JSON object"
                    )

                return BaseMessage(
                    **message_dict
                )

            except asyncio.IncompleteReadError:
                return None

            except asyncio.TimeoutError as exc:
                raise ConnectionError(
                    "Timed out while receiving message"
                ) from exc

            except ValueError:
                raise

            except OSError as exc:
                raise ConnectionError(
                    f"Failed to receive message: {exc}"
                ) from exc

    async def is_connected(
        self,
    ) -> bool:
        """Return whether the transport has an open writer."""

        writer = self.writer

        return (
            writer is not None
            and not writer.is_closing()
        )