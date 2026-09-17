"""End-to-end tests for resource-aware service execution."""

import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.node.agent import NodeAgent
from freemesh.node.resources import NodeResources
from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.service_requirements import ServiceRequirements


class FakeAuthenticator:
    """Simple authenticator for integration tests."""

    def authenticate(self, credentials):
        return credentials == "test-token"


class FakeTransport:
    """Minimal transport used for direct Agent handler testing."""

    def __init__(self):
        self.sent_messages = []
        self.connected = True

    async def send(self, message):
        self.sent_messages.append(message)

    async def is_connected(self):
        return self.connected


def test_controller_builds_resource_candidates():
    controller = Controller(
        authenticator=FakeAuthenticator()
    )

    controller.registry.register_node(
        node_id="node-a",
        hostname="node-a",
    )

    controller.registry.authenticate_node(
        "node-a",
        authenticated=True,
    )

    controller.registry.update_node_state(
        "node-a",
        "online",
    )

    controller.resource_registry.register_resources(
        "node-a",
        NodeResources(
            cpu_cores=8,
            cpu_usage_percent=10,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
            running_services=0,
        ),
    )

    candidates = controller._build_resource_candidates()

    assert len(candidates) == 1
    assert candidates[0].node_id == "node-a"
    assert candidates[0].resources is not None
    assert candidates[0].resources.cpu_cores == 8


def test_controller_selects_node_with_enough_resources():
    controller = Controller(
        authenticator=FakeAuthenticator()
    )

    controller.registry.register_node(
        node_id="small-node",
        hostname="small-node",
    )

    controller.registry.authenticate_node(
        "small-node",
        authenticated=True,
    )

    controller.registry.update_node_state(
        "small-node",
        "online",
    )

    controller.resource_registry.register_resources(
        "small-node",
        NodeResources(
            cpu_cores=2,
            cpu_usage_percent=20,
            memory_total_mb=2000,
            memory_used_mb=1500,
            disk_total_gb=20,
            disk_used_gb=10,
        ),
    )

    controller.registry.register_node(
        node_id="large-node",
        hostname="large-node",
    )

    controller.registry.authenticate_node(
        "large-node",
        authenticated=True,
    )

    controller.registry.update_node_state(
        "large-node",
        "online",
    )

    controller.resource_registry.register_resources(
        "large-node",
        NodeResources(
            cpu_cores=8,
            cpu_usage_percent=10,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    controller._active_nodes["small-node"] = object()
    controller._active_nodes["large-node"] = object()

    selected = controller.select_node_for_service(
        required_cpu_cores=4,
        required_memory_mb=4000,
        required_disk_gb=10,
    )

    assert selected is not None
    assert selected.node_id == "large-node"


def test_controller_rejects_service_when_no_node_has_capacity():
    controller = Controller(
        authenticator=FakeAuthenticator()
    )

    controller.registry.register_node(
        node_id="node-a",
        hostname="node-a",
    )

    controller.registry.authenticate_node(
        "node-a",
        authenticated=True,
    )

    controller.registry.update_node_state(
        "node-a",
        "online",
    )

    controller.resource_registry.register_resources(
        "node-a",
        NodeResources(
            cpu_cores=2,
            cpu_usage_percent=80,
            memory_total_mb=2000,
            memory_used_mb=1800,
            disk_total_gb=20,
            disk_used_gb=18,
        ),
    )

    controller._active_nodes["node-a"] = object()

    selected = controller.select_node_for_service(
        required_cpu_cores=8,
        required_memory_mb=16000,
        required_disk_gb=50,
    )

    assert selected is None


@pytest.mark.asyncio
async def test_agent_starts_service_with_requirements():
    agent = NodeAgent(
        node_id="node-a",
        authentication_token="test-token",
    )

    fake_transport = FakeTransport()
    agent.transport = fake_transport

    message = BaseMessage(
        type=MessageType.SERVICE_START,
        message_id="request-1",
        payload={
            "service_id": "service-a",
            "command": "python3 -c \"import time; time.sleep(30)\"",
            "requirements": {
                "cpu_cores": 1.5,
                "memory_mb": 512,
                "disk_gb": 2.0,
            },
            "request_id": "request-1",
        },
    )

    await agent._handle_service_start(message)

    assert len(fake_transport.sent_messages) == 1

    response = fake_transport.sent_messages[0]

    assert response.type == MessageType.SERVICE_START_RESPONSE
    assert response.payload["status"] == "started"
    assert response.payload["service_id"] == "service-a"
    assert response.payload["node_id"] == "node-a"

    assert response.payload["requirements"] == {
        "cpu_cores": 1.5,
        "memory_mb": 512,
        "disk_gb": 2.0,
    }

    service = agent._service_manager.get_service(
        "service-a"
    )

    assert service is not None
    assert service.node_id == "node-a"
    assert service.requirements == ServiceRequirements(
        cpu_cores=1.5,
        memory_mb=512,
        disk_gb=2.0,
    )

    await agent._service_manager.stop_service(
        "service-a"
    )


@pytest.mark.asyncio
async def test_agent_returns_requirements_in_service_status():
    agent = NodeAgent(
        node_id="node-a",
        authentication_token="test-token",
    )

    fake_transport = FakeTransport()
    agent.transport = fake_transport

    service = await agent._service_manager.start_service(
        service_id="service-status",
        command="python3 -c \"import time; time.sleep(30)\"",
    )

    service.requirements = ServiceRequirements(
        cpu_cores=2.0,
        memory_mb=1024,
        disk_gb=5.0,
    )

    service.node_id = "node-a"

    status_message = BaseMessage(
        type=MessageType.SERVICE_STATUS,
        message_id="status-request",
        payload={
            "service_id": "service-status",
            "request_id": "status-request",
        },
    )

    await agent._handle_service_status(
        status_message
    )

    assert len(fake_transport.sent_messages) == 1

    response = fake_transport.sent_messages[0]

    assert response.type == MessageType.SERVICE_STATUS_RESPONSE
    assert response.payload["status"] == "running"
    assert response.payload["node_id"] == "node-a"

    assert response.payload["requirements"] == {
        "cpu_cores": 2.0,
        "memory_mb": 1024,
        "disk_gb": 5.0,
    }

    await agent._service_manager.stop_service(
        "service-status"
    )


@pytest.mark.asyncio
async def test_agent_rejects_invalid_requirements_payload():
    agent = NodeAgent(
        node_id="node-a",
        authentication_token="test-token",
    )

    fake_transport = FakeTransport()
    agent.transport = fake_transport

    message = BaseMessage(
        type=MessageType.SERVICE_START,
        message_id="request-invalid",
        payload={
            "service_id": "service-invalid",
            "command": "python3 -c \"print('test')\"",
            "requirements": "invalid",
            "request_id": "request-invalid",
        },
    )

    await agent._handle_service_start(message)

    assert len(fake_transport.sent_messages) == 1

    response = fake_transport.sent_messages[0]

    assert response.type == MessageType.SERVICE_START_RESPONSE
    assert response.payload["status"] == "failed"
    assert response.payload["service_id"] == "service-invalid"
    assert "requirements must be an object" in (
        response.payload["error"]
    )


@pytest.mark.asyncio
async def test_controller_start_service_sends_requirements():
    controller = Controller(
        authenticator=FakeAuthenticator()
    )

    requirements = ServiceRequirements(
        cpu_cores=2.0,
        memory_mb=1024,
        disk_gb=4.0,
    )

    class CaptureTransport:
        def __init__(self):
            self.sent_message = None

        async def send(self, message):
            self.sent_message = message

    transport = CaptureTransport()

    controller._active_nodes["node-a"] = transport

    async def fake_wait():
        await asyncio.sleep(0)

    original_wait = asyncio.Event.wait

    event = asyncio.Event()

    async def immediate_wait(self):
        await fake_wait()

    controller._service_response_events = {
        "unused": event
    }

    request_id = None

    async def fake_send_and_response(message):
        nonlocal request_id

        request_id = message.payload["request_id"]

        response = BaseMessage(
            type=MessageType.SERVICE_START_RESPONSE,
            message_id="response-1",
            payload={
                "status": "started",
                "service_id": "service-a",
                "node_id": "node-a",
                "command": "python3 -c \"print('test')\"",
                "pid": 1234,
                "requirements": requirements.to_dict(),
                "request_id": request_id,
            },
        )

        controller._store_service_response(
            response,
            node_id="node-a",
        )

    transport.send = fake_send_and_response

    response = await controller.start_service(
        node_id="node-a",
        service_id="service-a",
        command="python3 -c \"print('test')\"",
        requirements=requirements,
    )

    assert response.payload["status"] == "started"
    assert request_id is not None

    registered = controller.service_registry.get_service(
        "service-a"
    )

    assert registered is not None
    assert registered.node_id == "node-a"
    assert registered.command == (
        "python3 -c \"print('test')\""
    )