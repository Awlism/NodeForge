"""End-to-end tests for real NodeForge service migration."""

import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_for_condition(
    condition,
    timeout: float = 15.0,
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
async def test_real_service_migration_e2e(
    monkeypatch,
):
    """Migrate a real running service from Node A to Node B."""

    token = "nodeforge-migration-e2e-token"

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
        heartbeat_timeout_seconds=5.0,
        authenticator=authenticator,
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node_a = None
    node_b = None

    node_a_task = None
    node_b_task = None

    service_id = "real-migration-e2e-service"

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
            node_id="migration-node-a",
            hostname="migration-host-a",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.1,
            heartbeat_interval_seconds=0.1,
        )

        node_b = NodeAgent(
            node_id="migration-node-b",
            hostname="migration-host-b",
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
                    "migration-node-a"
                ).state
                == NodeState.ONLINE
                and controller.registry.get_node(
                    "migration-node-b"
                ).state
                == NodeState.ONLINE
            )
        )

        await wait_for_condition(
            lambda: (
                controller.resource_registry.get_resources(
                    "migration-node-a"
                )
                is not None
                and controller.resource_registry.get_resources(
                    "migration-node-b"
                )
                is not None
            )
        )

        requirements = {
            "cpu_cores": 0.1,
            "memory_mb": 1,
            "disk_gb": 0.0,
        }

        command = (
            "python3 -c "
            "\"import time; time.sleep(60)\""
        )

        start_response = (
            await controller.start_service(
                node_id="migration-node-a",
                service_id=service_id,
                command=command,
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
            == "migration-node-a"
        )

        assert service.command == command

        assert service.pid == first_pid

        assert service.requirements.to_dict() == {
            "cpu_cores": 0.0,
            "memory_mb": 0,
            "disk_gb": 0.0,
        }

        service.requirements = (
            service.requirements.from_dict(
                requirements
            )
        )

        accounting = (
            controller.resource_accounting.get(
                service_id
            )
        )

        assert accounting is not None

        assert (
            accounting.node_id
            == "migration-node-a"
        )

        assert (
            accounting.requirements.cpu_cores
            == 0.0
        )

        status_before = (
            await controller.status_service(
                node_id="migration-node-a",
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

        migration_result = (
            await controller.migrate_service(
                service_id=service_id,
                failed_node_id="migration-node-a",
                timeout_seconds=10.0,
            )
        )

        assert migration_result is not None

        assert (
            migration_result.payload["status"]
            == "migrated"
        )

        assert (
            migration_result.payload[
                "source_node_id"
            ]
            == "migration-node-a"
        )

        assert (
            migration_result.payload[
                "target_node_id"
            ]
            == "migration-node-b"
        )

        new_pid = migration_result.payload.get(
            "pid"
        )

        assert new_pid is not None

        assert new_pid != first_pid

        await wait_for_condition(
            lambda: (
                controller.service_registry
                .get_service(service_id)
                is not None
                and controller.service_registry
                .get_service(service_id)
                .node_id
                == "migration-node-b"
            )
        )

        migrated_service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert migrated_service is not None

        assert (
            migrated_service.node_id
            == "migration-node-b"
        )

        assert (
            migrated_service.command
            == command
        )

        assert (
            migrated_service.pid
            == new_pid
        )

        assert (
            migrated_service.status
            == "running"
        )

        migration_record = (
            controller.migration_registry.get(
                service_id
            )
        )

        assert migration_record is not None

        assert (
            migration_record.source_node_id
            == "migration-node-a"
        )

        assert (
            migration_record.target_node_id
            == "migration-node-b"
        )

        assert (
            migration_record.status
            == "completed"
        )

        assert (
            migration_record.pid
            == new_pid
        )

        migrated_accounting = (
            controller.resource_accounting.get(
                service_id
            )
        )

        assert migrated_accounting is not None

        assert (
            migrated_accounting.node_id
            == "migration-node-b"
        )

        status_after = (
            await controller.status_service(
                node_id="migration-node-b",
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

        stop_response = (
            await controller.stop_service(
                node_id="migration-node-b",
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

        final_status = (
            await controller.status_service(
                node_id="migration-node-b",
                service_id=service_id,
                timeout_seconds=10.0,
            )
        )

        assert (
            final_status.payload["status"]
            == "not_found"
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