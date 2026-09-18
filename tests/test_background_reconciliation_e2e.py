"""End-to-end tests for background desired-state reconciliation."""

import asyncio
import contextlib
import os

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_until(
    condition,
    timeout: float = 15.0,
    interval: float = 0.1,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return

        await asyncio.sleep(interval)

    raise AssertionError(
        "Condition was not satisfied before timeout"
    )


@pytest.mark.asyncio
async def test_background_reconciliation_restores_missing_service():
    """Background reconciliation should restore a missing service."""

    token = "nodeforge-background-reconcile-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=5.0,
        authenticator=DevelopmentTokenAuthenticator(),
        reconciliation_interval_seconds=0.2,
    )

    controller_task = asyncio.create_task(controller.start())

    node = None
    node_task = None

    try:
        await wait_until(
            lambda: (
                controller.server is not None
                and controller.server.sockets is not None
            ),
            timeout=5.0,
        )

        controller_port = (
            controller.server.sockets[0]
            .getsockname()[1]
        )

        node = NodeAgent(
            node_id="background-reconcile-node",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.2,
            heartbeat_interval_seconds=0.2,
        )

        node_task = asyncio.create_task(node.start())

        await wait_until(
            lambda: node.get_state() == AgentState.READY,
            timeout=10.0,
        )

        await wait_until(
            lambda: (
                controller.registry.get_node(
                    "background-reconcile-node"
                )
                is not None
            ),
            timeout=5.0,
        )

        await wait_until(
            lambda: (
                controller.resource_registry.get_resources(
                    "background-reconcile-node"
                )
                is not None
            ),
            timeout=5.0,
        )

        service_id = "background-reconcile-service"
        command = "python -c \"import time; time.sleep(30)\""

        controller.create_service_intent(
            service_id=service_id,
            desired_state=DesiredState.RUNNING,
            command=command,
        )

        assert (
            controller.service_registry.get_service(service_id)
            is None
        )

        # The important part of D4:
        # We do NOT call controller.reconcile_service().
        # The background loop must do it automatically.

        await wait_until(
            lambda: (
                controller.service_registry.get_service(
                    service_id
                )
                is not None
            ),
            timeout=10.0,
        )

        service = controller.service_registry.get_service(service_id)

        assert service is not None
        assert service.node_id == "background-reconcile-node"
        assert service.status == "running"
        assert service.pid is not None

        await wait_until(
            lambda: (
                node._service_manager.get_service(service_id)
                is not None
            ),
            timeout=10.0,
        )

        node_service = node._service_manager.get_service(service_id)
        process = node._service_manager.get_process(service_id)

        assert node_service is not None
        assert process is not None
        assert node_service.status == "running"
        assert node_service.pid == service.pid
        assert process.returncode is None

    finally:
        if node is not None:
            with contextlib.suppress(Exception):
                await node.stop()

        if node_task is not None:
            node_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await node_task

        controller_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await controller_task

        with contextlib.suppress(Exception):
            await controller.stop()

        os.environ.pop("NODEFORGE_AUTH_TOKEN", None)