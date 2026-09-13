import asyncio
import os
from datetime import datetime, timezone, timedelta

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeRegistry, NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


@pytest.mark.asyncio
async def test_controller_node_registration_authentication_and_heartbeat():
    token = "test-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    authenticator = DevelopmentTokenAuthenticator()

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=authenticator,
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
            node_id="integration-node",
            hostname="integration-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(agent.start())

        try:
            for _ in range(100):
                node = controller.registry.get_node("integration-node")

                if (
                    node is not None
                    and node.authenticated
                    and node.state == NodeState.ONLINE
                    and node.last_heartbeat_time is not None
                ):
                    break

                await asyncio.sleep(0.05)

            node = controller.registry.get_node("integration-node")

            assert node is not None
            assert node.authenticated is True
            assert node.state == NodeState.ONLINE
            assert node.last_heartbeat_time is not None
            assert agent.get_state() == AgentState.READY

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


def test_node_registry_offline_detection():
    registry = NodeRegistry()

    node = registry.register_node(
        node_id="offline-test-node",
        hostname="offline-test-host",
    )

    node.last_heartbeat_time = (
        datetime.now(timezone.utc) - timedelta(seconds=10)
    )

    offline_nodes = registry.detect_offline_nodes(
        timeout_seconds=2.0
    )

    assert len(offline_nodes) == 1
    assert offline_nodes[0].node_id == "offline-test-node"

    registry.mark_offline("offline-test-node")

    updated_node = registry.get_node("offline-test-node")

    assert updated_node is not None
    assert updated_node.state == NodeState.OFFLINE


@pytest.mark.asyncio
async def test_node_reconnection():
    token = "reconnect-test-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    authenticator = DevelopmentTokenAuthenticator()

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=authenticator,
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
            node_id="reconnect-node",
            hostname="reconnect-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(agent.start())

        try:
            # Wait for the first successful connection.
            for _ in range(100):
                node = controller.registry.get_node("reconnect-node")

                if (
                    node is not None
                    and node.authenticated
                    and node.state == NodeState.ONLINE
                    and node.last_heartbeat_time is not None
                ):
                    break

                await asyncio.sleep(0.05)

            node = controller.registry.get_node("reconnect-node")

            assert node is not None
            assert node.authenticated is True
            assert node.state == NodeState.ONLINE
            assert agent.get_state() == AgentState.READY

            # Force the current TCP connection to close.
            await agent.transport.disconnect()

            # Wait for the agent to reconnect.
            reconnected = False

            for _ in range(100):
                node = controller.registry.get_node("reconnect-node")

                if (
                    node is not None
                    and node.authenticated
                    and node.state == NodeState.ONLINE
                    and agent.get_state() == AgentState.READY
                    and agent.transport is not None
                    and await agent.transport.is_connected()
                ):
                    reconnected = True
                    break

                await asyncio.sleep(0.05)

            assert reconnected is True

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