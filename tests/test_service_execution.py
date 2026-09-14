import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator
from freemesh.protocol.messages import MessageType


async def create_test_controller_and_agent():
    token = "service-test-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    server_task = asyncio.create_task(controller.start())

    for _ in range(50):
        if controller.server is not None:
            break
        await asyncio.sleep(0.01)

    assert controller.server is not None

    port = controller.server.sockets[0].getsockname()[1]

    agent = NodeAgent(
        node_id="service-node",
        hostname="service-host",
        controller_host="127.0.0.1",
        controller_port=port,
        authentication_token=token,
        heartbeat_interval_seconds=0.1,
        reconnect_delay_seconds=0.1,
    )

    agent_task = asyncio.create_task(agent.start())

    for _ in range(100):
        node = controller.registry.get_node("service-node")

        if (
            node is not None
            and node.authenticated
            and node.state == NodeState.ONLINE
            and agent.get_state() == AgentState.READY
        ):
            break

        await asyncio.sleep(0.05)

    node = controller.registry.get_node("service-node")

    assert node is not None
    assert node.authenticated is True
    assert node.state == NodeState.ONLINE
    assert agent.get_state() == AgentState.READY

    return controller, server_task, agent, agent_task


async def cleanup_test_controller_and_agent(
    controller,
    server_task,
    agent,
    agent_task,
):
    await agent.stop()

    if not agent_task.done():
        agent_task.cancel()

    try:
        await agent_task
    except asyncio.CancelledError:
        pass

    await controller.stop()

    if not server_task.done():
        server_task.cancel()

    try:
        await server_task
    except asyncio.CancelledError:
        pass


async def wait_for_service_status(
    controller,
    service_id,
    expected_status,
    timeout=2.0,
):
    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        response = await controller.status_service(
            node_id="service-node",
            service_id=service_id,
        )

        if response.payload.get("status") == expected_status:
            return response

        await asyncio.sleep(0.1)

    return await controller.status_service(
        node_id="service-node",
        service_id=service_id,
    )


@pytest.mark.asyncio
async def test_service_start_command():
    controller, server_task, agent, agent_task = (
        await create_test_controller_and_agent()
    )

    try:
        response = await controller.start_service(
            node_id="service-node",
            service_id="test-service",
            command='python -c "import time; time.sleep(5)"',
        )

        assert response is not None
        assert response.type == MessageType.SERVICE_START_RESPONSE
        assert response.payload["service_id"] == "test-service"
        assert response.payload["status"] == "started"

    finally:
        await cleanup_test_controller_and_agent(
            controller,
            server_task,
            agent,
            agent_task,
        )


@pytest.mark.asyncio
async def test_service_status_command():
    controller, server_task, agent, agent_task = (
        await create_test_controller_and_agent()
    )

    try:
        start_response = await controller.start_service(
            node_id="service-node",
            service_id="status-service",
            command='python -c "import time; time.sleep(5)"',
        )

        assert start_response.type == MessageType.SERVICE_START_RESPONSE
        assert start_response.payload["status"] == "started"

        status_response = await controller.status_service(
            node_id="service-node",
            service_id="status-service",
        )

        assert status_response is not None
        assert status_response.type == MessageType.SERVICE_STATUS_RESPONSE
        assert status_response.payload["service_id"] == "status-service"
        assert status_response.payload["status"] == "running"
        assert status_response.payload["pid"] is not None

        stop_response = await controller.stop_service(
            node_id="service-node",
            service_id="status-service",
        )

        assert stop_response is not None
        assert stop_response.type == MessageType.SERVICE_STOP_RESPONSE
        assert stop_response.payload["service_id"] == "status-service"
        assert stop_response.payload["status"] == "stopped"

        final_status_response = await controller.status_service(
            node_id="service-node",
            service_id="status-service",
        )

        assert final_status_response is not None
        assert (
            final_status_response.type
            == MessageType.SERVICE_STATUS_RESPONSE
        )
        assert (
            final_status_response.payload["service_id"]
            == "status-service"
        )
        assert final_status_response.payload["status"] == "not_found"

    finally:
        await cleanup_test_controller_and_agent(
            controller,
            server_task,
            agent,
            agent_task,
        )


@pytest.mark.asyncio
async def test_service_crash_and_auto_restart():
    controller, server_task, agent, agent_task = (
        await create_test_controller_and_agent()
    )

    try:
        response = await controller.start_service(
            node_id="service-node",
            service_id="crash-service",
            command='python -c "import sys; sys.exit(1)"',
        )

        assert response is not None
        assert response.type == MessageType.SERVICE_START_RESPONSE
        assert response.payload["service_id"] == "crash-service"
        assert response.payload["status"] == "started"

        restarted_response = await wait_for_service_status(
            controller,
            "crash-service",
            "running",
            timeout=2.0,
        )

        assert restarted_response.type == MessageType.SERVICE_STATUS_RESPONSE
        assert restarted_response.payload["status"] == "running"
        assert restarted_response.payload["pid"] is not None

        crashed_response = await wait_for_service_status(
            controller,
            "crash-service",
            "crashed",
            timeout=2.0,
        )

        assert crashed_response.type == MessageType.SERVICE_STATUS_RESPONSE
        assert crashed_response.payload["status"] == "crashed"

    finally:
        await cleanup_test_controller_and_agent(
            controller,
            server_task,
            agent,
            agent_task,
        )
        
@pytest.mark.asyncio
async def test_service_max_restart_attempts():
    controller, server_task, agent, agent_task = (
        await create_test_controller_and_agent()
    )

    try:
        response = await controller.start_service(
            node_id="service-node",
            service_id="restart-limit-service",
            command='python -c "import sys; sys.exit(1)"',
        )

        assert response is not None
        assert response.type == MessageType.SERVICE_START_RESPONSE
        assert response.payload["service_id"] == "restart-limit-service"
        assert response.payload["status"] == "started"

        await asyncio.sleep(1.5)

        status_response = await controller.status_service(
            node_id="service-node",
            service_id="restart-limit-service",
        )

        assert status_response is not None
        assert status_response.type == MessageType.SERVICE_STATUS_RESPONSE
        assert status_response.payload["status"] == "crashed"
        assert status_response.payload["restart_attempts"] == 3
        assert status_response.payload["max_restart_attempts"] == 3

    finally:
        await cleanup_test_controller_and_agent(
            controller,
            server_task,
            agent,
            agent_task,
        )