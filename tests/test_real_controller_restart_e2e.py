"""End-to-end test for real Controller restart recovery."""

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
async def test_real_controller_restart_recovers_state(
    tmp_path,
):
    """Controller restart should recover intent and service metadata."""

    token = "nodeforge-real-restart-token"
    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    database_path = (
        tmp_path / "nodeforge-restart.db"
    )

    controller_one = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=5.0,
        authenticator=DevelopmentTokenAuthenticator(),
        database_path=str(database_path),
    )

    controller_one_task = asyncio.create_task(
        controller_one.start()
    )

    node = None
    node_task = None
    controller_two = None
    controller_two_task = None

    try:
        # -------------------------------------------------
        # Start Controller #1
        # -------------------------------------------------

        await wait_until(
            lambda: (
                controller_one.server is not None
                and controller_one.server.sockets is not None
            ),
            timeout=5.0,
        )

        controller_port = (
            controller_one.server.sockets[0]
            .getsockname()[1]
        )

        # -------------------------------------------------
        # Start real Node
        # -------------------------------------------------

        node = NodeAgent(
            node_id="restart-recovery-node",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.2,
            heartbeat_interval_seconds=0.2,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        await wait_until(
            lambda: (
                node.get_state()
                == AgentState.READY
            ),
            timeout=10.0,
        )

        await wait_until(
            lambda: (
                controller_one.registry.get_node(
                    "restart-recovery-node"
                )
                is not None
            ),
            timeout=5.0,
        )

        # -------------------------------------------------
        # Create persistent Desired State
        # -------------------------------------------------

        service_id = "restart-recovery-service"

        command = (
            "python -c "
            "\"import time; time.sleep(60)\""
        )

        controller_one.create_service_intent(
            service_id=service_id,
            desired_state=DesiredState.RUNNING,
            command=command,
        )

        # -------------------------------------------------
        # Reconcile and start real service
        # -------------------------------------------------

        result = await controller_one.reconcile_service(
            service_id
        )

        assert result.action == "start"
        assert result.changed is True
        assert result.reason == "service_missing"

        # -------------------------------------------------
        # Wait for service registration
        # -------------------------------------------------

        await wait_until(
            lambda: (
                controller_one.service_registry.get_service(
                    service_id
                )
                is not None
            ),
            timeout=10.0,
        )

        service_one = (
            controller_one.service_registry.get_service(
                service_id
            )
        )

        assert service_one is not None
        assert service_one.node_id == (
            "restart-recovery-node"
        )
        assert service_one.status == "running"
        assert service_one.pid is not None

        # -------------------------------------------------
        # Verify persistent metadata exists
        # -------------------------------------------------

        metadata_one = (
            controller_one.service_metadata_store.get(
                service_id
            )
        )

        assert metadata_one is not None
        assert metadata_one["service_id"] == service_id
        assert metadata_one["node_id"] == (
            "restart-recovery-node"
        )
        assert metadata_one["status"] == "running"
        assert metadata_one["pid"] is not None

        # -------------------------------------------------
        # Verify persistent intent exists
        # -------------------------------------------------

        intent_one = (
            controller_one.get_service_intent(
                service_id
            )
        )

        assert intent_one is not None
        assert (
            intent_one.desired_state
            == DesiredState.RUNNING
        )
        assert intent_one.command == command

        # -------------------------------------------------
        # Stop Controller #1
        # -------------------------------------------------

        await controller_one.stop()

        if not controller_one_task.done():
            controller_one_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await controller_one_task

        controller_one = None

        # -------------------------------------------------
        # Start Controller #2
        # -------------------------------------------------

        controller_two = Controller(
            host="127.0.0.1",
            port=0,
            heartbeat_timeout_seconds=5.0,
            authenticator=DevelopmentTokenAuthenticator(),
            database_path=str(database_path),
        )

        # -------------------------------------------------
        # Verify persistent state immediately recovered
        # -------------------------------------------------

        recovered_intent = (
            controller_two.get_service_intent(
                service_id
            )
        )

        assert recovered_intent is not None
        assert (
            recovered_intent.service_id
            == service_id
        )
        assert (
            recovered_intent.desired_state
            == DesiredState.RUNNING
        )
        assert recovered_intent.command == command

        recovered_service = (
            controller_two.service_registry.get_service(
                service_id
            )
        )

        assert recovered_service is not None
        assert recovered_service.service_id == service_id
        assert recovered_service.node_id == (
            "restart-recovery-node"
        )
        assert recovered_service.status == "running"
        assert recovered_service.pid is not None

        recovered_metadata = (
            controller_two.service_metadata_store.get(
                service_id
            )
        )

        assert recovered_metadata is not None
        assert (
            recovered_metadata["service_id"]
            == service_id
        )
        assert recovered_metadata["node_id"] == (
            "restart-recovery-node"
        )
        assert recovered_metadata["status"] == "running"

        # -------------------------------------------------
        # Start Controller #2
        # -------------------------------------------------

        controller_two_task = asyncio.create_task(
            controller_two.start()
        )

        await wait_until(
            lambda: (
                controller_two.server is not None
                and controller_two.server.sockets is not None
            ),
            timeout=5.0,
        )

        # -------------------------------------------------
        # Reconnect the real Node to Controller #2
        # -------------------------------------------------

        controller_two_port = (
            controller_two.server.sockets[0]
            .getsockname()[1]
        )

        # The current NodeAgent keeps its configured
        # controller port, so stop it and create a fresh
        # real NodeAgent for Controller #2.

        await node.stop()

        if not node_task.done():
            node_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await node_task

        node = NodeAgent(
            node_id="restart-recovery-node",
            controller_host="127.0.0.1",
            controller_port=controller_two_port,
            authentication_token=token,
            reconnect_delay_seconds=0.2,
            heartbeat_interval_seconds=0.2,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        await wait_until(
            lambda: (
                node.get_state()
                == AgentState.READY
            ),
            timeout=10.0,
        )

        await wait_until(
            lambda: (
                controller_two.registry.get_node(
                    "restart-recovery-node"
                )
                is not None
            ),
            timeout=5.0,
        )

        # -------------------------------------------------
        # Reconcile recovered state
        # -------------------------------------------------

        result_after_restart = (
            await controller_two.reconcile_service(
                service_id
            )
        )

        assert result_after_restart.service_id == (
            service_id
        )

        # The service metadata was recovered as
        # already running, so reconciliation should
        # not blindly start a duplicate process.
        assert result_after_restart.action == "none"
        assert result_after_restart.changed is False
        assert result_after_restart.reason == (
            "already_running"
        )

        # -------------------------------------------------
        # Final persistence verification
        # -------------------------------------------------

        final_service = (
            controller_two.service_registry.get_service(
                service_id
            )
        )

        assert final_service is not None
        assert final_service.status == "running"
        assert final_service.node_id == (
            "restart-recovery-node"
        )

    finally:
        # -------------------------------------------------
        # Cleanup Node
        # -------------------------------------------------

        if node is not None:
            with contextlib.suppress(Exception):
                await node.stop()

        if node_task is not None:
            node_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await node_task

        # -------------------------------------------------
        # Cleanup Controller #1
        # -------------------------------------------------

        if controller_one is not None:
            with contextlib.suppress(Exception):
                await controller_one.stop()

        if controller_one_task is not None:
            controller_one_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await controller_one_task

        # -------------------------------------------------
        # Cleanup Controller #2
        # -------------------------------------------------

        if controller_two is not None:
            with contextlib.suppress(Exception):
                await controller_two.stop()

        if controller_two_task is not None:
            controller_two_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await controller_two_task

        os.environ.pop(
            "NODEFORGE_AUTH_TOKEN",
            None,
        )