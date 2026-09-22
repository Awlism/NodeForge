"""End-to-end resource-aware service placement tests."""

import asyncio
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import (
    DevelopmentTokenAuthenticator,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


async def wait_for_node(
    controller,
    node_id,
):
    for _ in range(100):
        node = controller.registry.get_node(
            node_id
        )

        if (
            node is not None
            and node.authenticated
            and node.state == NodeState.ONLINE
        ):
            return

        await asyncio.sleep(0.05)

    raise AssertionError(
        f"Node {node_id} not ready"
    )


@pytest.mark.asyncio
async def test_resource_aware_service_start():
    token = "resource-e2e-token"

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

    agent = None
    agent_task = None

    try:
        for _ in range(100):
            if controller.server is not None:
                break

            await asyncio.sleep(0.02)

        assert controller.server is not None

        port = controller.server.sockets[
            0
        ].getsockname()[1]

        agent = NodeAgent(
            node_id="resource-node",
            hostname="resource-node-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        agent_task = asyncio.create_task(
            agent.start()
        )

        await wait_for_node(
            controller,
            "resource-node",
        )

        assert (
            agent.get_state()
            == AgentState.READY
        )

        requirements = ServiceRequirements(
            cpu_cores=0.1,
            memory_mb=32,
            disk_gb=0.0,
        )

        response = (
            await controller.start_service_auto(
                service_id=(
                    "resource-aware-service"
                ),
                command=(
                    'python -c "import time; '
                    'time.sleep(10)"'
                ),
                required_cpu_cores=(
                    requirements.cpu_cores
                ),
                required_memory_mb=(
                    requirements.memory_mb
                ),
                required_disk_gb=(
                    requirements.disk_gb
                ),
            )
        )

        assert response is not None
        assert response.payload["status"] == (
            "started"
        )

        service = (
            controller.service_registry.get_service(
                "resource-aware-service"
            )
        )

        assert service is not None
        assert service.node_id == (
            "resource-node"
        )

        reservation = (
            controller.resource_accounting.get(
                "resource-aware-service"
            )
        )

        assert reservation is not None
        assert reservation.node_id == (
            "resource-node"
        )

        assert reservation.cpu_cores == 0.1
        assert reservation.memory_mb == 32

        await controller.stop_service(
            node_id="resource-node",
            service_id=(
                "resource-aware-service"
            ),
        )

        assert (
            controller.resource_accounting.get(
                "resource-aware-service"
            )
            is None
        )

    finally:
        if agent is not None:
            await agent.stop()

        if (
            agent_task is not None
            and not agent_task.done()
        ):
            agent_task.cancel()

            try:
                await agent_task
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