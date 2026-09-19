import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.controller.service_intent import DesiredState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator
from freemesh.service_requirements import ServiceRequirements


async def wait_for_condition(
    condition,
    timeout: float = 15.0,
    interval: float = 0.05,
):
    deadline = (
        asyncio.get_running_loop().time()
        + timeout
    )

    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return True

        await asyncio.sleep(interval)

    return condition()


@pytest.mark.asyncio
async def test_failover_respects_capacity_constraints(
    monkeypatch,
):
    token = "d610-capacity-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=1.0,
        authenticator=DevelopmentTokenAuthenticator(),
        reconciliation_interval_seconds=60.0,
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    nodes = {}
    node_tasks = {}

    service_ids = [
        "d610-service-1",
        "d610-service-2",
    ]

    command = (
        "python3 -c "
        "\"import time; time.sleep(120)\""
    )

    try:
        # -----------------------------------------------------
        # Start Controller
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: controller.server is not None
        )

        port = controller.server.sockets[0].getsockname()[1]

        # -----------------------------------------------------
        # Create:
        #
        # A = source / failed node
        # B = recovery node
        # -----------------------------------------------------

        for node_id, hostname in (
            ("d610-node-a", "d610-host-a"),
            ("d610-node-b", "d610-host-b"),
        ):
            node = NodeAgent(
                node_id=node_id,
                hostname=hostname,
                controller_host="127.0.0.1",
                controller_port=port,
                authentication_token=token,
                heartbeat_interval_seconds=0.1,
                reconnect_delay_seconds=0.1,
            )

            nodes[node_id] = node

            node_tasks[node_id] = asyncio.create_task(
                node.start()
            )

        # -----------------------------------------------------
        # Wait for nodes.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: all(
                node.get_state()
                == AgentState.READY
                for node in nodes.values()
            )
        )

        assert await wait_for_condition(
            lambda: all(
                (
                    controller.registry.get_node(
                        node_id
                    )
                    is not None
                    and controller.registry.get_node(
                        node_id
                    ).state
                    == NodeState.ONLINE
                )
                for node_id in nodes
            )
        )

        # -----------------------------------------------------
        # Wait for resource reports.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: all(
                controller.resource_registry.get_resources(
                    node_id
                )
                is not None
                for node_id in nodes
            )
        )

        source_resources = (
            controller.resource_registry.get_resources(
                "d610-node-a"
            )
        )

        target_resources = (
            controller.resource_registry.get_resources(
                "d610-node-b"
            )
        )

        assert source_resources is not None
        assert target_resources is not None

        # -----------------------------------------------------
        # Make each service require 60% of the target node's
        # allowed CPU capacity.
        #
        # Two services would therefore require 120% and
        # cannot both fit on Node B.
        # -----------------------------------------------------

        required_cpu = max(
            target_resources.cpu_cores
            * 0.60,
            0.1,
        )

        requirements = ServiceRequirements(
            cpu_cores=required_cpu,
        )

        # -----------------------------------------------------
        # Create Desired State.
        # -----------------------------------------------------

        for service_id in service_ids:
            controller.create_service_intent(
                service_id=service_id,
                command=command,
                requirements=requirements,
                desired_state=DesiredState.RUNNING,
            )

        # -----------------------------------------------------
        # Start both services on Node A.
        # -----------------------------------------------------

        for service_id in service_ids:
            started = await controller.start_service(
                node_id="d610-node-a",
                service_id=service_id,
                command=command,
                requirements=requirements,
            )

            assert (
                started.payload["status"]
                == "started"
            )

        # -----------------------------------------------------
        # Verify both are running on A.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: all(
                (
                    controller.service_registry
                    .get_service(service_id)
                    is not None
                    and controller.service_registry
                    .get_service(service_id).status
                    == "running"
                    and controller.service_registry
                    .get_service(service_id).node_id
                    == "d610-node-a"
                )
                for service_id in service_ids
            )
        )

        # -----------------------------------------------------
        # Verify reservations on A.
        # -----------------------------------------------------

        assert (
            controller.resource_accounting.service_count(
                "d610-node-a"
            )
            == 2
        )

        # -----------------------------------------------------
        # Fail Node A.
        # -----------------------------------------------------

        await nodes["d610-node-a"].stop()

        task = node_tasks["d610-node-a"]

        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        node_tasks["d610-node-a"] = None

        # -----------------------------------------------------
        # Wait until A is OFFLINE.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "d610-node-a"
                )
                is not None
                and controller.registry.get_node(
                    "d610-node-a"
                ).state
                == NodeState.OFFLINE
            ),
            timeout=5.0,
        )

        assert (
            controller.registry.get_node(
                "d610-node-b"
            ).state
            == NodeState.ONLINE
        )

        # -----------------------------------------------------
        # Capacity constraint:
        #
        # At most ONE service can migrate to B.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                controller.resource_accounting
                .service_count("d610-node-b")
                <= 1
            ),
            timeout=5.0,
        )

        # -----------------------------------------------------
        # Wait until recovery processing has settled.
        # -----------------------------------------------------

        await asyncio.sleep(2.0)

        services = [
            controller.service_registry.get_service(
                service_id
            )
            for service_id in service_ids
        ]

        assert all(
            service is not None
            for service in services
        )

        # -----------------------------------------------------
        # Critical assertions:
        #
        # Node B must never contain both reservations.
        # -----------------------------------------------------

        assert (
            controller.resource_accounting
            .service_count("d610-node-b")
            <= 1
        )

        assert (
            controller.resource_accounting
            .service_count()
            <= 1
        )

        # -----------------------------------------------------
        # If a service migrated, it must actually be running
        # on B.
        # -----------------------------------------------------

        migrated_services = [
            service
            for service in services
            if service.node_id == "d610-node-b"
        ]

        assert len(migrated_services) <= 1

        for service in migrated_services:
            assert service.status == "running"

            reservation = (
                controller.resource_accounting
                .get(service.service_id)
            )

            assert reservation is not None
            assert reservation.node_id == "d610-node-b"

            assert await wait_for_condition(
                lambda service_id=service.service_id: (
                    nodes["d610-node-b"]
                    ._service_manager
                    .get_service(service_id)
                    is not None
                ),
                timeout=10.0,
            )

        # -----------------------------------------------------
        # The other service must NOT be falsely marked as
        # successfully migrated to B.
        # -----------------------------------------------------

        blocked_services = [
            service
            for service in services
            if service.node_id != "d610-node-b"
        ]

        assert len(blocked_services) >= 1

        # -----------------------------------------------------
        # Desired State must remain RUNNING for both services.
        #
        # This is important:
        # "migration failed because capacity is unavailable"
        # must NOT silently change the user's desired state.
        # -----------------------------------------------------

        for service_id in service_ids:
            intent = controller.get_service_intent(
                service_id
            )

            assert intent is not None

            assert (
                intent.desired_state
                == DesiredState.RUNNING
            )

            assert (
                intent.requirements
                == requirements
            )

        # -----------------------------------------------------
        # Failed node must have no active reservation.
        # -----------------------------------------------------

        assert (
            controller.resource_accounting
            .service_count("d610-node-a")
            == 0
        )

    finally:
        for node in nodes.values():
            try:
                await node.stop()
            except Exception:
                pass

        for node_id, task in node_tasks.items():
            if task is None:
                continue

            if not task.done():
                task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

        await controller.stop()

        if not controller_task.done():
            controller_task.cancel()

        try:
            await controller_task
        except asyncio.CancelledError:
            pass