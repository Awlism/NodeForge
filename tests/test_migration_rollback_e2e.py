"""End-to-end migration rollback tests for NodeForge."""

import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_for_condition(
    condition,
    timeout: float = 10.0,
    interval: float = 0.05,
):
    deadline = (
        asyncio.get_running_loop().time()
        + timeout
    )

    while (
        asyncio.get_running_loop().time()
        < deadline
    ):
        if condition():
            return True

        await asyncio.sleep(interval)

    return condition()


@pytest.mark.asyncio
async def test_controller_migration_rollback_cleans_target(
    monkeypatch,
):
    token = "d612-rollback-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=5.0,
        authenticator=DevelopmentTokenAuthenticator(),
        reconciliation_interval_seconds=60.0,
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node_a = None
    node_b = None
    node_a_task = None
    node_b_task = None

    service_id = "d612-rollback-service"

    command = (
        "python3 -c "
        "\"import time; time.sleep(120)\""
    )

    try:
        assert await wait_for_condition(
            lambda: controller.server is not None
        )

        port = (
            controller.server
            .sockets[0]
            .getsockname()[1]
        )

        node_a = NodeAgent(
            node_id="d612-node-a",
            hostname="d612-host-a",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        node_b = NodeAgent(
            node_id="d612-node-b",
            hostname="d612-host-b",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        node_a_task = asyncio.create_task(
            node_a.start()
        )

        node_b_task = asyncio.create_task(
            node_b.start()
        )

        assert await wait_for_condition(
            lambda: (
                node_a.get_state()
                == AgentState.READY
                and node_b.get_state()
                == AgentState.READY
            )
        )

        controller.create_service_intent(
            service_id=service_id,
            command=command,
            desired_state=DesiredState.RUNNING,
        )

        started = await controller.start_service(
            node_id="d612-node-a",
            service_id=service_id,
            command=command,
        )

        assert (
            started.payload["status"]
            == "started"
        )

        assert await wait_for_condition(
            lambda: (
                controller.service_registry
                .get_service(service_id)
                is not None
                and controller.service_registry
                .get_service(service_id)
                .node_id
                == "d612-node-a"
            )
        )

        original_service = (
            controller.service_registry
            .get_service(service_id)
        )

        assert original_service is not None

        original_pid = original_service.pid

        reservation = (
            controller.resource_accounting
            .get(service_id)
        )

        assert reservation is not None
        assert reservation.node_id == "d612-node-a"

        # -----------------------------------------------------
        # Force target verification to fail.
        #
        # The target service will really start on Node B,
        # but Controller verification will report failure.
        # -----------------------------------------------------

        original_status_service = (
            controller.status_service
        )

        async def failing_status_service(
            node_id,
            service_id,
            timeout_seconds=10.0,
        ):
            if node_id == "d612-node-b":
                from freemesh.protocol.messages import (
                    BaseMessage,
                    MessageType,
                )

                return BaseMessage(
                    type=MessageType.SERVICE_STATUS_RESPONSE,
                    message_id="d612-forced-failure",
                    payload={
                        "status": "failed",
                        "service_id": service_id,
                        "node_id": node_id,
                    },
                )

            return await original_status_service(
                node_id=node_id,
                service_id=service_id,
                timeout_seconds=timeout_seconds,
            )

        controller.status_service = (
            failing_status_service
        )

        migration_result = (
            await controller.migrate_service(
                service_id=service_id,
                failed_node_id="d612-node-a",
                timeout_seconds=10.0,
            )
        )

        assert migration_result is not None

        assert (
            migration_result.payload["status"]
            == "verification_failed"
        )

        # -----------------------------------------------------
        # Source reservation must remain intact.
        # -----------------------------------------------------

        reservation = (
            controller.resource_accounting
            .get(service_id)
        )

        assert reservation is not None

        assert (
            reservation.node_id
            == "d612-node-a"
        )

        # -----------------------------------------------------
        # Registry must roll back to source.
        # -----------------------------------------------------

        service = (
            controller.service_registry
            .get_service(service_id)
        )

        assert service is not None

        assert (
            service.node_id
            == "d612-node-a"
        )

        assert service.pid == original_pid

        # -----------------------------------------------------
        # Desired state must remain RUNNING.
        # -----------------------------------------------------

        intent = (
            controller.get_service_intent(
                service_id
            )
        )

        assert intent is not None

        assert (
            intent.desired_state
            == DesiredState.RUNNING
        )

        # -----------------------------------------------------
        # Target service must be cleaned up.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                node_b._service_manager
                .get_service(service_id)
                is None
            ),
            timeout=5.0,
        )

    finally:
        controller.status_service = (
            original_status_service
            if "original_status_service" in locals()
            else controller.status_service
        )

        if node_a is not None:
            try:
                await node_a.stop()
            except Exception:
                pass

        if node_b is not None:
            try:
                await node_b.stop()
            except Exception:
                pass

        if node_a_task is not None:
            if not node_a_task.done():
                node_a_task.cancel()

            try:
                await node_a_task
            except asyncio.CancelledError:
                pass

        if node_b_task is not None:
            if not node_b_task.done():
                node_b_task.cancel()

            try:
                await node_b_task
            except asyncio.CancelledError:
                pass

        await controller.stop()

        if not controller_task.done():
            controller_task.cancel()

        try:
            await controller_task
        except asyncio.CancelledError:
            pass