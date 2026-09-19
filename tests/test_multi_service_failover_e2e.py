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
async def test_multi_service_failover_between_multiple_nodes(
    monkeypatch,
):
    token = "d69-multi-failover-token"

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
        "d69-service-1",
        "d69-service-2",
        "d69-service-3",
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
        # Create three nodes:
        #
        # A = source / failed node
        # B = recovery node
        # C = recovery node
        # -----------------------------------------------------

        for node_id, hostname in (
            ("d69-node-a", "d69-host-a"),
            ("d69-node-b", "d69-host-b"),
            ("d69-node-c", "d69-host-c"),
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
        # Wait for all nodes to become READY.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: all(
                node.get_state()
                == AgentState.READY
                for node in nodes.values()
            )
        )

        # -----------------------------------------------------
        # Wait for all nodes to become ONLINE.
        # -----------------------------------------------------

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
        # Wait for fresh resource reports.
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
                "d69-node-a"
            )
        )

        assert source_resources is not None

        # -----------------------------------------------------
        # Use a reservation large enough that one healthy
        # node cannot accept all three services.
        #
        # 40% of CPU capacity per service means:
        #
        # - one service fits
        # - two services fit
        # - three services do not fit
        #
        # Therefore failover must use more than one target
        # node when all three services are recovered.
        # -----------------------------------------------------

        required_cpu = max(
            source_resources.cpu_cores * 0.40,
            0.1,
        )

        requirements = ServiceRequirements(
            cpu_cores=required_cpu,
        )

        # -----------------------------------------------------
        # Create persistent Desired State for all services.
        # -----------------------------------------------------

        for service_id in service_ids:
            controller.create_service_intent(
                service_id=service_id,
                command=command,
                requirements=requirements,
                desired_state=DesiredState.RUNNING,
            )

        # -----------------------------------------------------
        # Start all services on Node A.
        #
        # We intentionally start them directly on A so that
        # all services share the same failed source node.
        # -----------------------------------------------------

        for service_id in service_ids:
            started = await controller.start_service(
                node_id="d69-node-a",
                service_id=service_id,
                command=command,
                requirements=requirements,
            )

            assert (
                started.payload["status"]
                == "started"
            )

        # -----------------------------------------------------
        # Verify all services are running on Node A.
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
                    == "d69-node-a"
                )
                for service_id in service_ids
            )
        )

        # -----------------------------------------------------
        # Verify all reservations are initially on A.
        # -----------------------------------------------------

        assert all(
            (
                controller.resource_accounting
                .get(service_id)
                is not None
                and controller.resource_accounting
                .get(service_id).node_id
                == "d69-node-a"
            )
            for service_id in service_ids
        )

        assert (
            controller.resource_accounting.service_count(
                "d69-node-a"
            )
            == 3
        )

        # -----------------------------------------------------
        # Fail Node A.
        # -----------------------------------------------------

        await nodes["d69-node-a"].stop()

        task = node_tasks["d69-node-a"]

        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        node_tasks["d69-node-a"] = None

        # -----------------------------------------------------
        # Controller must detect A as OFFLINE.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "d69-node-a"
                )
                is not None
                and controller.registry.get_node(
                    "d69-node-a"
                ).state
                == NodeState.OFFLINE
            ),
            timeout=5.0,
        )

        # -----------------------------------------------------
        # B and C must remain ONLINE.
        # -----------------------------------------------------

        assert (
            controller.registry.get_node(
                "d69-node-b"
            ).state
            == NodeState.ONLINE
        )

        assert (
            controller.registry.get_node(
                "d69-node-c"
            ).state
            == NodeState.ONLINE
        )

        # -----------------------------------------------------
        # IMPORTANT:
        #
        # Do NOT manually call migrate_service().
        #
        # Offline detection must automatically recover
        # every service from the failed node.
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
                    in {
                        "d69-node-b",
                        "d69-node-c",
                    }
                )
                for service_id in service_ids
            ),
            timeout=15.0,
        )

        # -----------------------------------------------------
        # Collect final placement.
        # -----------------------------------------------------

        placements = {
            service_id: (
                controller.service_registry
                .get_service(service_id)
                .node_id
            )
            for service_id in service_ids
        }

        target_nodes = set(
            placements.values()
        )

        # -----------------------------------------------------
        # Critical D6.9 assertion:
        #
        # Because one node cannot accept all three reservations,
        # recovery must use BOTH healthy nodes.
        # -----------------------------------------------------

        assert target_nodes == {
            "d69-node-b",
            "d69-node-c",
        }

        # -----------------------------------------------------
        # Every service must be RUNNING.
        # -----------------------------------------------------

        for service_id in service_ids:
            service = (
                controller.service_registry
                .get_service(service_id)
            )

            assert service is not None
            assert service.status == "running"
            assert service.node_id in {
                "d69-node-b",
                "d69-node-c",
            }

        # -----------------------------------------------------
        # Verify Resource Accounting moved every reservation
        # away from failed Node A.
        # -----------------------------------------------------

        assert (
            controller.resource_accounting.service_count(
                "d69-node-a"
            )
            == 0
        )

        assert (
            controller.resource_accounting.service_count(
                "d69-node-b"
            )
            >= 1
        )

        assert (
            controller.resource_accounting.service_count(
                "d69-node-c"
            )
            >= 1
        )

        for service_id in service_ids:
            reservation = (
                controller.resource_accounting
                .get(service_id)
            )

            assert reservation is not None

            assert reservation.node_id in {
                "d69-node-b",
                "d69-node-c",
            }

        # -----------------------------------------------------
        # Verify actual runtime service exists on the target
        # Node for every recovered service.
        # -----------------------------------------------------

        for service_id in service_ids:
            target_node_id = placements[
                service_id
            ]

            target_node = nodes[
                target_node_id
            ]

            assert await wait_for_condition(
                lambda service_id=service_id,
                target_node=target_node: (
                    target_node._service_manager
                    .get_service(service_id)
                    is not None
                ),
                timeout=10.0,
            )

        # -----------------------------------------------------
        # Verify actual runtime status through Controller.
        # -----------------------------------------------------

        for service_id in service_ids:
            target_node_id = placements[
                service_id
            ]

            response = await controller.status_service(
                node_id=target_node_id,
                service_id=service_id,
            )

            assert (
                response.payload["status"]
                == "running"
            )

        # -----------------------------------------------------
        # Desired State must survive the failure.
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
        # Verify no reservation remains on failed node.
        # -----------------------------------------------------

        assert (
            controller.resource_accounting
            .list_node_reservations(
                "d69-node-a"
            )
            == []
        )

        # -----------------------------------------------------
        # Verify exactly three reservations exist.
        # -----------------------------------------------------

        assert (
            controller.resource_accounting
            .service_count()
            == 3
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