import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
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

    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return True

        await asyncio.sleep(interval)

    return condition()


@pytest.mark.asyncio
async def test_real_service_failover_between_nodes(
    monkeypatch,
):
    token = "d68-failover-token"

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

    node_a = None
    node_b = None
    node_a_task = None
    node_b_task = None

    service_id = "d68-failover-service"

    command = (
        "python3 -c "
        "\"import time; time.sleep(120)\""
    )

    try:
        assert await wait_for_condition(
            lambda: controller.server is not None
        )

        port = controller.server.sockets[0].getsockname()[1]

        node_a = NodeAgent(
            node_id="d68-node-a",
            hostname="d68-host-a",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        node_b = NodeAgent(
            node_id="d68-node-b",
            hostname="d68-host-b",
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

        # -----------------------------------------------------
        # Wait for both nodes to become ready.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                node_a.get_state()
                == AgentState.READY
                and node_b.get_state()
                == AgentState.READY
            )
        )

        assert await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "d68-node-a"
                )
                is not None
                and controller.registry.get_node(
                    "d68-node-b"
                )
                is not None
                and controller.registry.get_node(
                    "d68-node-a"
                ).state
                == NodeState.ONLINE
                and controller.registry.get_node(
                    "d68-node-b"
                ).state
                == NodeState.ONLINE
            )
        )

        # -----------------------------------------------------
        # Create persistent desired state.
        # -----------------------------------------------------

        controller.create_service_intent(
            service_id=service_id,
            command=command,
            desired_state=DesiredState.RUNNING,
        )

        # -----------------------------------------------------
        # Start service on Node A.
        # -----------------------------------------------------

        started = await controller.start_service(
            node_id="d68-node-a",
            service_id=service_id,
            command=command,
        )

        assert started.payload["status"] == "started"

        assert await wait_for_condition(
            lambda: (
                controller.service_registry
                .get_service(service_id)
                is not None
                and controller.service_registry
                .get_service(service_id).status
                == "running"
            )
        )

        service = (
            controller.service_registry
            .get_service(service_id)
        )

        assert service is not None
        assert service.node_id == "d68-node-a"
        assert service.status == "running"

        # -----------------------------------------------------
        # Verify reservation is on Node A.
        # -----------------------------------------------------

        reservation = (
            controller.resource_accounting
            .get(service_id)
        )

        assert reservation is not None
        assert reservation.node_id == "d68-node-a"

        # -----------------------------------------------------
        # Simulate complete failure of Node A.
        # -----------------------------------------------------

        await node_a.stop()

        if node_a_task is not None:
            if not node_a_task.done():
                node_a_task.cancel()

            try:
                await node_a_task
            except asyncio.CancelledError:
                pass

        node_a_task = None

        # -----------------------------------------------------
        # Controller must detect Node A as offline.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "d68-node-a"
                )
                is not None
                and controller.registry.get_node(
                    "d68-node-a"
                ).state
                == NodeState.OFFLINE
            ),
            timeout=5.0,
        )

        # -----------------------------------------------------
        # Node B must remain online.
        # -----------------------------------------------------

        assert (
            controller.registry.get_node(
                "d68-node-b"
            ).state
            == NodeState.ONLINE
        )

        # -----------------------------------------------------
        # IMPORTANT:
        # Do NOT call controller.migrate_service().
        #
        # The Controller's offline detection loop must
        # automatically recover the service.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                controller.service_registry
                .get_service(service_id)
                is not None
                and controller.service_registry
                .get_service(service_id).status
                == "running"
                and controller.service_registry
                .get_service(service_id).node_id
                == "d68-node-b"
            ),
            timeout=10.0,
        )

        # -----------------------------------------------------
        # Verify Controller service state.
        # -----------------------------------------------------

        service = (
            controller.service_registry
            .get_service(service_id)
        )

        assert service is not None
        assert service.status == "running"
        assert service.node_id == "d68-node-b"

        # -----------------------------------------------------
        # Verify Resource Accounting moved reservation.
        # -----------------------------------------------------

        reservation = (
            controller.resource_accounting
            .get(service_id)
        )

        assert reservation is not None
        assert reservation.node_id == "d68-node-b"

        # -----------------------------------------------------
        # Verify actual runtime on Node B.
        # -----------------------------------------------------

        assert await wait_for_condition(
            lambda: (
                node_b._service_manager
                .get_service(service_id)
                is not None
            ),
            timeout=10.0,
        )

        recovered = await controller.status_service(
            node_id="d68-node-b",
            service_id=service_id,
        )

        assert (
            recovered.payload["status"]
            == "running"
        )

        # -----------------------------------------------------
        # Verify Desired State survived the failure.
        # -----------------------------------------------------

        intent = controller.get_service_intent(
            service_id
        )

        assert intent is not None

        assert (
            intent.desired_state
            == DesiredState.RUNNING
        )

    finally:
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