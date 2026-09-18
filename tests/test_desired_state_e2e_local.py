"""End-to-end test for desired-state reconciliation."""

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
    """Wait until a synchronous condition becomes true."""

    deadline = (
        asyncio.get_running_loop().time()
        + timeout
    )

    while (
        asyncio.get_running_loop().time()
        < deadline
    ):
        if condition():
            return

        await asyncio.sleep(interval)

    raise AssertionError(
        "Condition was not satisfied before timeout"
    )


@pytest.mark.asyncio
async def test_desired_state_reconciles_missing_service():
    """RUNNING desired state should start a missing service."""

    token = "nodeforge-desired-state-token"

    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=5.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node = None
    node_task = None

    try:
        # -------------------------------------------------
        # Controller
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Node
        # -------------------------------------------------

        node = NodeAgent(
            node_id="desired-state-node",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.2,
            heartbeat_interval_seconds=0.2,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        # -------------------------------------------------
        # Wait for Node connection
        # -------------------------------------------------

        await wait_until(
            lambda: (
                node.get_state()
                == AgentState.READY
            ),
            timeout=10.0,
        )

        await wait_until(
            lambda: (
                controller.registry.get_node(
                    "desired-state-node"
                )
                is not None
            ),
            timeout=5.0,
        )

        await wait_until(
            lambda: (
                controller.resource_registry.get_resources(
                    "desired-state-node"
                )
                is not None
            ),
            timeout=5.0,
        )

        # -------------------------------------------------
        # Create Desired State
        # -------------------------------------------------

        controller.create_service_intent(
            service_id="desired-state-service",
            desired_state=DesiredState.RUNNING,
            command=(
                "python -c "
                "\"import time; time.sleep(30)\""
            ),
        )

        intent = (
            controller.get_service_intent(
                "desired-state-service"
            )
        )

        assert intent is not None

        assert (
            intent.desired_state
            == DesiredState.RUNNING
        )

        # -------------------------------------------------
        # Verify service does not exist yet
        # -------------------------------------------------

        assert (
            controller.service_registry.get_service(
                "desired-state-service"
            )
            is None
        )

        assert (
            node._service_manager.get_service(
                "desired-state-service"
            )
            is None
        )

        # -------------------------------------------------
        # Reconcile Desired → Actual
        # -------------------------------------------------

        result = await controller.reconcile_service(
            "desired-state-service"
        )

        assert result.action == "start"
        assert result.changed is True
        assert result.reason == "service_missing"

        # -------------------------------------------------
        # Wait for Controller service registration
        # -------------------------------------------------

        await wait_until(
            lambda: (
                controller.service_registry.get_service(
                    "desired-state-service"
                )
                is not None
            ),
            timeout=10.0,
        )

        service = (
            controller.service_registry.get_service(
                "desired-state-service"
            )
        )

        assert service is not None

        assert (
            service.node_id
            == "desired-state-node"
        )

        assert (
            service.status.value
            == "running"
        )

        assert service.pid is not None

        # -------------------------------------------------
        # Verify actual NodeAgent service
        # -------------------------------------------------

        await wait_until(
            lambda: (
                node._service_manager.get_service(
                    "desired-state-service"
                )
                is not None
            ),
            timeout=10.0,
        )

        node_service = (
            node._service_manager.get_service(
                "desired-state-service"
            )
        )

        assert node_service is not None

        assert (
            node_service.status.value
            == "running"
        )

        assert node_service.pid is not None

        assert (
            node_service.pid
            == service.pid
        )

        # -------------------------------------------------
        # Verify actual OS process
        # -------------------------------------------------

        process = (
            node._service_manager.get_process(
                "desired-state-service"
            )
        )

        assert process is not None

        assert process.returncode is None

    finally:
        if node is not None:
            with contextlib.suppress(Exception):
                await node.stop()

        if node_task is not None:
            node_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await node_task

        controller_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await controller_task

        with contextlib.suppress(Exception):
            await controller.stop()

        os.environ.pop(
            "NODEFORGE_AUTH_TOKEN",
            None,
        )