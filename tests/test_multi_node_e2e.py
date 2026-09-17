"""End-to-end tests for multi-node NodeForge execution."""

import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_for_condition(
    condition,
    timeout: float = 10.0,
    interval: float = 0.05,
) -> None:
    """Wait until an asynchronous condition becomes true."""

    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return

        await asyncio.sleep(interval)

    raise AssertionError(
        "Condition was not satisfied before timeout"
    )


@pytest.mark.asyncio
async def test_multi_node_controller_execution(monkeypatch):
    """Test real Controller communication with two NodeAgents."""

    token = "nodeforge-multi-node-e2e-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

    authenticator = DevelopmentTokenAuthenticator()

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=5.0,
        authenticator=authenticator,
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node_a_task = None
    node_b_task = None

    node_a = None
    node_b = None

    try:
        await wait_for_condition(
            lambda: controller.server is not None
        )

        sockets = controller.server.sockets

        assert sockets

        controller_port = sockets[0].getsockname()[1]

        node_a = NodeAgent(
            node_id="e2e-node-a",
            hostname="e2e-host-a",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.1,
            heartbeat_interval_seconds=0.1,
        )

        node_b = NodeAgent(
            node_id="e2e-node-b",
            hostname="e2e-host-b",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.1,
            heartbeat_interval_seconds=0.1,
        )

        node_a_task = asyncio.create_task(
            node_a.start()
        )

        node_b_task = asyncio.create_task(
            node_b.start()
        )

        await wait_for_condition(
            lambda: (
                node_a is not None
                and node_a.get_state()
                == AgentState.READY
                and node_b is not None
                and node_b.get_state()
                == AgentState.READY
            )
        )

        await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "e2e-node-a"
                ).state
                == NodeState.ONLINE
                and controller.registry.get_node(
                    "e2e-node-b"
                ).state
                == NodeState.ONLINE
            )
        )

        await wait_for_condition(
            lambda: (
                controller.resource_registry.get_resources(
                    "e2e-node-a"
                )
                is not None
                and controller.resource_registry.get_resources(
                    "e2e-node-b"
                )
                is not None
            )
        )

        service_id = "multi-node-e2e-service"

        command = (
            "python3 -c "
            "\"import time; time.sleep(30)\""
        )

        response = await controller.start_service_auto(
            service_id=service_id,
            command=command,
            required_cpu_cores=0.1,
            required_memory_mb=1,
            required_disk_gb=0.0,
            timeout_seconds=10.0,
        )

        assert response.type.value == (
            "service_start_response"
        )

        assert response.payload["status"] == "started"

        selected_node_id = response.payload["node_id"]

        assert selected_node_id in {
            "e2e-node-a",
            "e2e-node-b",
        }

        service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert service is not None

        assert service.node_id == selected_node_id

        assert service.command == command

        assert service.pid is not None

        status_response = (
            await controller.status_service(
                node_id=selected_node_id,
                service_id=service_id,
                timeout_seconds=10.0,
            )
        )

        assert status_response.payload["status"] == (
            "running"
        )

        assert (
            status_response.payload["pid"]
            == service.pid
        )

        other_node_id = (
            "e2e-node-b"
            if selected_node_id == "e2e-node-a"
            else "e2e-node-a"
        )

        other_resources = (
            controller.resource_registry.get_resources(
                other_node_id
            )
        )

        assert other_resources is not None

        assert (
            other_node_id
            != selected_node_id
        )

    finally:
        if node_a is not None:
            await node_a.stop()

        if node_b is not None:
            await node_b.stop()

        if node_a_task is not None:
            node_a_task.cancel()

            try:
                await node_a_task
            except asyncio.CancelledError:
                pass

        if node_b_task is not None:
            node_b_task.cancel()

            try:
                await node_b_task
            except asyncio.CancelledError:
                pass

        if controller.server is not None:
            await controller.stop()

        if not controller_task.done():
            controller_task.cancel()

        try:
            await controller_task
        except asyncio.CancelledError:
            pass