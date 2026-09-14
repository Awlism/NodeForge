import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.security.auth import DevelopmentTokenAuthenticator


@pytest.mark.asyncio
async def test_service_start_command():
    token = "service-test-token"
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
            node_id="service-node",
            hostname="service-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(agent.start())

        try:
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

            service_message = BaseMessage(
                type=MessageType.SERVICE_START,
                message_id="test-service-start",
                payload={
                    "service_id": "test-service",
                    "command": "python -c \"import time; time.sleep(5)\"",
                },
            )

            await controller._active_nodes["service-node"].send(service_message)

            response = await controller._active_nodes["service-node"].receive()

            assert response is not None
            assert response.type == MessageType.SERVICE_START_RESPONSE
            assert response.payload["service_id"] == "test-service"
            assert response.payload["status"] == "started"

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