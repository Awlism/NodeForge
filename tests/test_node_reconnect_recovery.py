import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_for_node_state(
    controller: Controller,
    node_id: str,
    state: NodeState,
    timeout: float = 5.0,
):
    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        node = controller.registry.get_node(node_id)

        if node is not None and node.state == state:
            return node

        await asyncio.sleep(0.05)

    return controller.registry.get_node(node_id)


async def wait_for_agent_state(
    agent: NodeAgent,
    state: AgentState,
    timeout: float = 5.0,
):
    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        if agent.get_state() == state:
            return True

        await asyncio.sleep(0.05)

    return False


@pytest.mark.asyncio
async def test_node_reconnect_restores_online_state():
    token = "d63-reconnect-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    server_task = asyncio.create_task(
        controller.start()
    )

    agent = None
    agent_task = None

    try:
        for _ in range(100):
            if controller.server is not None:
                break

            await asyncio.sleep(0.01)

        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        agent = NodeAgent(
            node_id="d63-reconnect-node",
            hostname="d63-reconnect-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(
            agent.start()
        )

        node = await wait_for_node_state(
            controller,
            "d63-reconnect-node",
            NodeState.ONLINE,
        )

        assert node is not None
        assert node.authenticated is True
        assert agent.get_state() == AgentState.READY

        await agent.transport.disconnect()

        await asyncio.sleep(0.2)

        node = await wait_for_node_state(
            controller,
            "d63-reconnect-node",
            NodeState.ONLINE,
        )

        assert node is not None
        assert node.authenticated is True
        assert agent.get_state() == AgentState.READY

    finally:
        if agent is not None:
            await agent.stop()

        if agent_task is not None and not agent_task.done():
            agent_task.cancel()

        if agent_task is not None:
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


@pytest.mark.asyncio
async def test_node_reconnect_reauthenticates():
    token = "d63-reauth-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    server_task = asyncio.create_task(
        controller.start()
    )

    agent = None
    agent_task = None

    try:
        for _ in range(100):
            if controller.server is not None:
                break

            await asyncio.sleep(0.01)

        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        agent = NodeAgent(
            node_id="d63-reauth-node",
            hostname="d63-reauth-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(
            agent.start()
        )

        node = await wait_for_node_state(
            controller,
            "d63-reauth-node",
            NodeState.ONLINE,
        )

        assert node is not None
        assert node.authenticated is True

        await agent.transport.disconnect()

        await asyncio.sleep(0.2)

        node = await wait_for_node_state(
            controller,
            "d63-reauth-node",
            NodeState.ONLINE,
        )

        assert node is not None
        assert node.authenticated is True
        assert agent.get_state() == AgentState.READY

    finally:
        if agent is not None:
            await agent.stop()

        if agent_task is not None and not agent_task.done():
            agent_task.cancel()

        if agent_task is not None:
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


@pytest.mark.asyncio
async def test_reconnected_node_can_receive_fresh_resources():
    token = "d63-resource-recovery-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    server_task = asyncio.create_task(
        controller.start()
    )

    agent = None
    agent_task = None

    try:
        for _ in range(100):
            if controller.server is not None:
                break

            await asyncio.sleep(0.01)

        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        agent = NodeAgent(
            node_id="d63-resource-node",
            hostname="d63-resource-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(
            agent.start()
        )

        node = await wait_for_node_state(
            controller,
            "d63-resource-node",
            NodeState.ONLINE,
        )

        assert node is not None
        assert agent.get_state() == AgentState.READY

        resources = controller.resource_registry.get_resources(
            "d63-resource-node"
        )

        assert resources is not None

        await agent.transport.disconnect()

        await asyncio.sleep(0.2)

        node = await wait_for_node_state(
            controller,
            "d63-resource-node",
            NodeState.ONLINE,
        )

        assert node is not None
        assert node.authenticated is True
        assert agent.get_state() == AgentState.READY

        recovered_resources = (
            controller.resource_registry.get_resources(
                "d63-resource-node"
            )
        )

        assert recovered_resources is not None

    finally:
        if agent is not None:
            await agent.stop()

        if agent_task is not None and not agent_task.done():
            agent_task.cancel()

        if agent_task is not None:
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