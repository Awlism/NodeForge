import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


@pytest.mark.asyncio
async def test_controller_service_health_monitor_updates_registry():
    token = "health-test-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    server_task = asyncio.create_task(controller.start())

    try:
        for _ in range(50):
            if controller.server is not None:
                break
            await asyncio.sleep(0.01)

        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        agent = NodeAgent(
            node_id="health-node",
            hostname="health-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(agent.start())

        try:
            for _ in range(100):
                node = controller.registry.get_node("health-node")

                if (
                    node is not None
                    and node.authenticated
                    and node.state == NodeState.ONLINE
                    and agent.get_state() == AgentState.READY
                ):
                    break

                await asyncio.sleep(0.05)

            node = controller.registry.get_node("health-node")

            assert node is not None
            assert node.authenticated is True
            assert node.state == NodeState.ONLINE
            assert agent.get_state() == AgentState.READY

            response = await controller.start_service(
                node_id="health-node",
                service_id="health-service",
                command="python -c \"import time; time.sleep(30)\"",
            )

            assert response.payload["status"] == "started"

            service = controller.service_registry.get_service(
                "health-service"
            )

            assert service is not None
            assert service.node_id == "health-node"

        finally:
            await agent.stop()

            if not agent_task.done():
                agent_task.cancel()

            try:
                await agent_task
            except asyncio.CancelledError:
                pass

    finally:
        await controller.stop()

        if not server_task.done():
            server_task.cancel()

        try:
            await server_task
        except asyncio.CancelledError:
            pass