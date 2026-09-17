import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


@pytest.mark.asyncio
async def test_service_failover_between_two_nodes():
    token = "failover-test-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    server_task = asyncio.create_task(controller.start())

    agent_a = None
    agent_b = None
    agent_a_task = None
    agent_b_task = None

    try:
        for _ in range(100):
            if controller.server is not None:
                break
            await asyncio.sleep(0.01)

        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        agent_a = NodeAgent(
            node_id="failover-node-a",
            hostname="failover-host-a",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_b = NodeAgent(
            node_id="failover-node-b",
            hostname="failover-host-b",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_a_task = asyncio.create_task(agent_a.start())
        agent_b_task = asyncio.create_task(agent_b.start())

        for _ in range(150):
            node_a = controller.registry.get_node(
                "failover-node-a"
            )
            node_b = controller.registry.get_node(
                "failover-node-b"
            )

            if (
                node_a is not None
                and node_b is not None
                and node_a.state == NodeState.ONLINE
                and node_b.state == NodeState.ONLINE
                and node_a.authenticated
                and node_b.authenticated
                and agent_a.get_state() == AgentState.READY
                and agent_b.get_state() == AgentState.READY
            ):
                break

            await asyncio.sleep(0.05)

        node_a = controller.registry.get_node(
            "failover-node-a"
        )
        node_b = controller.registry.get_node(
            "failover-node-b"
        )

        assert node_a is not None
        assert node_b is not None

        assert node_a.state == NodeState.ONLINE
        assert node_b.state == NodeState.ONLINE

        assert node_a.authenticated is True
        assert node_b.authenticated is True

        response = await controller.start_service(
            node_id="failover-node-a",
            service_id="failover-service",
            command=(
                "python -c "
                "\"import time; time.sleep(0.2)\""
            ),
        )

        assert response.payload["status"] == "started"

        service = controller.service_registry.get_service(
            "failover-service"
        )

        assert service is not None
        assert service.node_id == "failover-node-a"

        for _ in range(200):
            failure = controller.failure_manager.get_failure(
                "failover-service"
            )

            service = controller.service_registry.get_service(
                "failover-service"
            )

            if (
                failure is None
                and service is not None
                and service.node_id == "failover-node-b"
                and service.status == "running"
            ):
                break

            await asyncio.sleep(0.05)

        service = controller.service_registry.get_service(
            "failover-service"
        )

        assert service is not None
        assert service.node_id == "failover-node-b"
        assert service.status == "running"

        assert (
            controller.failure_manager.get_failure(
                "failover-service"
            )
            is None
        )

    finally:
        if agent_a is not None:
            await agent_a.stop()

        if agent_b is not None:
            await agent_b.stop()

        if agent_a_task is not None and not agent_a_task.done():
            agent_a_task.cancel()

        if agent_b_task is not None and not agent_b_task.done():
            agent_b_task.cancel()

        if agent_a_task is not None:
            try:
                await agent_a_task
            except asyncio.CancelledError:
                pass

        if agent_b_task is not None:
            try:
                await agent_b_task
            except asyncio.CancelledError:
                pass

        await controller.stop()

        if not server_task.done():
            server_task.cancel()

        try:
            await server_task
        except asyncio.CancelledError:
            pass