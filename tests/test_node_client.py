"""Integration tests for the Controller-side NodeClient."""

import asyncio

import pytest

from freemesh.controller.node_client import NodeClient
from freemesh.protocol.messages import BaseMessage, MessageType


class FakeNodeServer:
    """Small real TCP server used to exercise NodeClient."""

    def __init__(self) -> None:
        self.server = None
        self.host = "127.0.0.1"
        self.port = 0
        self.received_messages: list[BaseMessage] = []

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            0,
        )

        sockets = self.server.sockets

        if not sockets:
            raise RuntimeError(
                "server did not expose a listening socket"
            )

        self.port = sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self.server is None:
            return

        self.server.close()
        await self.server.wait_closed()
        self.server = None

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                header = await reader.readexactly(4)

                length = int.from_bytes(
                    header,
                    byteorder="big",
                )

                data = await reader.readexactly(length)

                message = BaseMessage.from_json(
                    data.decode("utf-8")
                )

                self.received_messages.append(message)

                response = self._create_response(message)

                encoded = response.to_json().encode(
                    "utf-8"
                )

                writer.write(
                    len(encoded).to_bytes(
                        4,
                        byteorder="big",
                    )
                )
                writer.write(encoded)

                await writer.drain()

        except (
            asyncio.IncompleteReadError,
            ConnectionResetError,
        ):
            pass
        finally:
            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

    @staticmethod
    def _create_response(
        message: BaseMessage,
    ) -> BaseMessage:
        if message.type == MessageType.AUTHENTICATE:
            return BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                payload={
                    "authenticated": True,
                },
            )

        if message.type == MessageType.SERVICE_START:
            return BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                payload={
                    "service_id": message.payload[
                        "service_id"
                    ],
                    "status": "running",
                    "pid": 12345,
                    "requirements": message.payload.get(
                        "requirements",
                        {},
                    ),
                },
            )

        if message.type == MessageType.SERVICE_STOP:
            return BaseMessage(
                type=MessageType.SERVICE_STOP_RESPONSE,
                payload={
                    "service_id": message.payload[
                        "service_id"
                    ],
                    "status": "stopped",
                },
            )

        if message.type == MessageType.SERVICE_STATUS:
            return BaseMessage(
                type=MessageType.SERVICE_STATUS_RESPONSE,
                payload={
                    "service_id": message.payload[
                        "service_id"
                    ],
                    "status": "running",
                    "pid": 12345,
                },
            )

        if message.type == MessageType.HEARTBEAT:
            return BaseMessage(
                type=MessageType.HEARTBEAT_RESPONSE,
                payload={
                    "node_id": message.payload[
                        "node_id"
                    ],
                    "status": "online",
                },
            )

        if message.type == MessageType.RESOURCE_REPORT:
            return BaseMessage(
                type=MessageType.RESOURCE_REPORT_RESPONSE,
                payload={
                    "node_id": message.payload[
                        "node_id"
                    ],
                    "resources": {},
                },
            )

        return BaseMessage(
            type=MessageType.ERROR,
            payload={
                "error": "unsupported message type",
            },
        )


@pytest.fixture
async def node_server():
    server = FakeNodeServer()
    await server.start()

    yield server

    await server.stop()


@pytest.mark.asyncio
async def test_node_client_connects_and_disconnects(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    assert client.connected is False

    await client.connect()

    assert client.connected is True
    assert client.authenticated is False

    await client.disconnect()

    assert client.connected is False
    assert client.authenticated is False


@pytest.mark.asyncio
async def test_node_client_authentication(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    await client.connect()

    response = await client.authenticate(
        token="development-token"
    )

    assert response.type == MessageType.AUTHENTICATE_RESPONSE
    assert response.payload["authenticated"] is True
    assert client.authenticated is True

    assert (
        node_server.received_messages[0].type
        == MessageType.AUTHENTICATE
    )

    await client.disconnect()


@pytest.mark.asyncio
async def test_node_client_starts_service(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    await client.connect()

    requirements = {
        "cpu_cores": 1.0,
        "memory_mb": 128,
        "disk_gb": 1.0,
    }

    response = await client.start_service(
        service_id="service-a",
        command="python -c 'print(1)'",
        requirements=requirements,
    )

    assert response.type == MessageType.SERVICE_START_RESPONSE
    assert response.payload["service_id"] == "service-a"
    assert response.payload["status"] == "running"
    assert response.payload["pid"] == 12345
    assert response.payload["requirements"] == requirements

    message = node_server.received_messages[-1]

    assert message.type == MessageType.SERVICE_START
    assert message.payload["node_id"] == "node-a"
    assert message.payload["service_id"] == "service-a"
    assert message.payload["command"] == "python -c 'print(1)'"
    assert message.payload["requirements"] == requirements

    await client.disconnect()


@pytest.mark.asyncio
async def test_node_client_stops_service(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    await client.connect()

    response = await client.stop_service(
        service_id="service-a"
    )

    assert response.type == MessageType.SERVICE_STOP_RESPONSE
    assert response.payload["service_id"] == "service-a"
    assert response.payload["status"] == "stopped"

    message = node_server.received_messages[-1]

    assert message.type == MessageType.SERVICE_STOP
    assert message.payload["node_id"] == "node-a"
    assert message.payload["service_id"] == "service-a"

    await client.disconnect()


@pytest.mark.asyncio
async def test_node_client_reads_service_status(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    await client.connect()

    response = await client.get_service_status(
        service_id="service-a"
    )

    assert response.type == MessageType.SERVICE_STATUS_RESPONSE
    assert response.payload["service_id"] == "service-a"
    assert response.payload["status"] == "running"
    assert response.payload["pid"] == 12345

    await client.disconnect()


@pytest.mark.asyncio
async def test_node_client_heartbeat(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    await client.connect()

    response = await client.send_heartbeat()

    assert response.type == MessageType.HEARTBEAT_RESPONSE
    assert response.payload["node_id"] == "node-a"
    assert response.payload["status"] == "online"

    await client.disconnect()


@pytest.mark.asyncio
async def test_node_client_resource_report(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    await client.connect()

    response = await client.request_resources()

    assert response.type == MessageType.RESOURCE_REPORT_RESPONSE
    assert response.payload["node_id"] == "node-a"

    await client.disconnect()


@pytest.mark.asyncio
async def test_node_client_requires_connection():
    client = NodeClient(
        node_id="node-a",
        host="127.0.0.1",
        port=9999,
    )

    with pytest.raises(ConnectionError):
        await client.send_heartbeat()


@pytest.mark.asyncio
async def test_node_client_context_manager(
    node_server: FakeNodeServer,
):
    client = NodeClient(
        node_id="node-a",
        host=node_server.host,
        port=node_server.port,
    )

    async with client:
        assert client.connected is True

        response = await client.send_heartbeat()

        assert response.type == MessageType.HEARTBEAT_RESPONSE

    assert client.connected is False