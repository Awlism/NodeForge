"""End-to-end tests for automatic NodeForge self-healing."""

import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator
from freemesh.service_requirements import ServiceRequirements


async def wait_for_condition(
    condition,
    timeout: float = 20.0,
    interval: float = 0.05,
) -> None:
    """Wait until a condition becomes true."""

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
async def test_automatic_self_healing_e2e(
    monkeypatch,
):
    """Automatically migrate a service after its node goes offline."""

    token = "nodeforge-self-healing-e2e-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

    authenticator = (
        DevelopmentTokenAuthenticator()
    )

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=0.4,
        authenticator=authenticator,
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node_a = None
    node_b = None

    node_a_task = None
    node_b_task = None

    service_id = "automatic-self-healing-service"

    try:
        await wait_for_condition(
            lambda: controller.server is not None
        )

        assert controller.server is not None

        sockets = controller.server.sockets

        assert sockets

        controller_port = (
            sockets[0].getsockname()[1]
        )

        node_a = NodeAgent(
            node_id="self-healing-node-a",
            hostname="self-healing-host-a",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.1,
            heartbeat_interval_seconds=0.1,
        )

        node_b = NodeAgent(
            node_id="self-healing-node-b",
            hostname="self-healing-host-b",
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
                node_a.get_state()
                == AgentState.READY
                and node_b.get_state()
                == AgentState.READY
            )
        )

        await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "self-healing-node-a"
                ).state
                == NodeState.ONLINE
                and controller.registry.get_node(
                    "self-healing-node-b"
                ).state
                == NodeState.ONLINE
            )
        )

        await wait_for_condition(
            lambda: (
                controller.resource_registry.get_resources(
                    "self-healing-node-a"
                )
                is not None
                and controller.resource_registry.get_resources(
                    "self-healing-node-b"
                )
                is not None
            )
        )

        requirements = ServiceRequirements(
            cpu_cores=0.1,
            memory_mb=1,
            disk_gb=0.0,
        )

        command = (
            "python3 -c "
            "\"import time; time.sleep(60)\""
        )

        start_response = (
            await controller.start_service(
                node_id="self-healing-node-a",
                service_id=service_id,
                command=command,
                requirements=requirements,
                timeout_seconds=10.0,
            )
        )

        assert (
            start_response.payload["status"]
            == "started"
        )

        first_pid = (
            start_response.payload.get("pid")
        )

        assert first_pid is not None

        service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert service is not None

        assert (
            service.node_id
            == "self-healing-node-a"
        )

        assert service.command == command

        assert service.pid == first_pid

        assert (
            service.requirements
            == requirements
        )

        reservation = (
            controller.resource_accounting.get(
                service_id
            )
        )

        assert reservation is not None

        assert (
            reservation.node_id
            == "self-healing-node-a"
        )

        status_before = (
            await controller.status_service(
                node_id="self-healing-node-a",
                service_id=service_id,
                timeout_seconds=10.0,
            )
        )

        assert (
            status_before.payload["status"]
            == "running"
        )

        assert (
            status_before.payload["pid"]
            == first_pid
        )

        # The actual failure event.
        #
        # We do NOT call migrate_service().
        # NodeForge must detect the failure itself.
        await node_a.stop()

        await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "self-healing-node-a"
                ).state
                == NodeState.OFFLINE
            ),
            timeout=10.0,
        )

        await wait_for_condition(
            lambda: (
                controller.service_registry
                .get_service(service_id)
                is not None
                and controller.service_registry
                .get_service(service_id)
                .node_id
                == "self-healing-node-b"
            ),
            timeout=15.0,
        )

        migrated_service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert migrated_service is not None

        assert (
            migrated_service.node_id
            == "self-healing-node-b"
        )

        assert (
            migrated_service.command
            == command
        )

        assert (
            migrated_service.status
            == "running"
        )

        new_pid = migrated_service.pid

        assert new_pid is not None

        assert new_pid != first_pid

        migration_record = (
            controller.migration_registry.get(
                service_id
            )
        )

        assert migration_record is not None

        assert (
            migration_record.source_node_id
            == "self-healing-node-a"
        )

        assert (
            migration_record.target_node_id
            == "self-healing-node-b"
        )

        assert (
            migration_record.status
            == "migrated"
        )

        assert (
            migration_record.pid
            == new_pid
        )

        reservation_after = (
            controller.resource_accounting.get(
                service_id
            )
        )

        assert reservation_after is not None

        assert (
            reservation_after.node_id
            == "self-healing-node-b"
        )

        status_after = (
            await controller.status_service(
                node_id="self-healing-node-b",
                service_id=service_id,
                timeout_seconds=10.0,
            )
        )

        assert (
            status_after.payload["status"]
            == "running"
        )

        assert (
            status_after.payload["pid"]
            == new_pid
        )

        assert (
            controller.resource_registry.get_resources(
                "self-healing-node-a"
            )
            is None
        )

        assert (
            controller.resource_registry.get_resources(
                "self-healing-node-b"
            )
            is not None
        )

        stop_response = (
            await controller.stop_service(
                node_id="self-healing-node-b",
                service_id=service_id,
                timeout_seconds=10.0,
            )
        )

        assert (
            stop_response.payload["status"]
            in {
                "stopped",
                "success",
            }
        )

        assert (
            controller.resource_accounting.get(
                service_id
            )
            is None
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