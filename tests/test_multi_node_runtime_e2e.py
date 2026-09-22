"""End-to-end multi-node runtime validation."""

import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import (
    DevelopmentTokenAuthenticator,
)


async def wait_for_node(
    controller,
    node_id,
    timeout=5.0,
):
    deadline = (
        asyncio.get_running_loop().time()
        + timeout
    )

    while (
        asyncio.get_running_loop().time()
        < deadline
    ):
        node = controller.registry.get_node(
            node_id
        )

        if (
            node is not None
            and node.authenticated
            and node.state == NodeState.ONLINE
        ):
            return node

        await asyncio.sleep(0.05)

    raise AssertionError(
        f"Node {node_id} did not become online"
    )


@pytest.mark.asyncio
async def test_two_nodes_register_and_execute_services():
    token = "multi-node-test-token"

    os.environ[
        "NODEFORGE_AUTH_TOKEN"
    ] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=(
            DevelopmentTokenAuthenticator()
        ),
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    agent_a = None
    agent_b = None
    agent_a_task = None
    agent_b_task = None

    try:
        for _ in range(100):
            if controller.server is not None:
                break

            await asyncio.sleep(0.02)

        assert controller.server is not None

        port = controller.server.sockets[
            0
        ].getsockname()[1]

        agent_a = NodeAgent(
            node_id="multi-node-a",
            hostname="multi-node-a-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_b = NodeAgent(
            node_id="multi-node-b",
            hostname="multi-node-b-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_a_task = asyncio.create_task(
            agent_a.start()
        )

        agent_b_task = asyncio.create_task(
            agent_b.start()
        )

        await wait_for_node(
            controller,
            "multi-node-a",
        )

        await wait_for_node(
            controller,
            "multi-node-b",
        )

        assert (
            agent_a.get_state()
            == AgentState.READY
        )

        assert (
            agent_b.get_state()
            == AgentState.READY
        )

        first = await controller.start_service(
            node_id="multi-node-a",
            service_id="multi-node-service-a",
            command=(
                'python -c "import time; '
                'time.sleep(10)"'
            ),
        )

        second = await controller.start_service(
            node_id="multi-node-b",
            service_id="multi-node-service-b",
            command=(
                'python -c "import time; '
                'time.sleep(10)"'
            ),
        )

        assert first.payload["status"] == "started"
        assert second.payload["status"] == "started"

        status_a = await controller.status_service(
            node_id="multi-node-a",
            service_id="multi-node-service-a",
        )

        status_b = await controller.status_service(
            node_id="multi-node-b",
            service_id="multi-node-service-b",
        )

        assert status_a.payload["status"] == "running"
        assert status_b.payload["status"] == "running"

        await controller.stop_service(
            node_id="multi-node-a",
            service_id="multi-node-service-a",
        )

        await controller.stop_service(
            node_id="multi-node-b",
            service_id="multi-node-service-b",
        )

    finally:
        if agent_a is not None:
            await agent_a.stop()

        if agent_b is not None:
            await agent_b.stop()

        for task in (
            agent_a_task,
            agent_b_task,
        ):
            if task is not None and not task.done():
                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    pass

        await controller.stop()

        if (
            controller_task is not None
            and not controller_task.done()
        ):
            controller_task.cancel()

            try:
                await controller_task
            except asyncio.CancelledError:
                pass