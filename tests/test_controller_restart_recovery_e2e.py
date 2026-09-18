"""End-to-end tests for recovery after a real Controller restart."""

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
async def test_real_controller_restart_recovers_nodes_service_and_intent(
    tmp_path,
    monkeypatch,
):
    """Recover a running service after a real Controller restart."""

    token = "nodeforge-controller-restart-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

    database_path = (
        tmp_path / "nodeforge-controller.db"
    )

    authenticator = (
        DevelopmentTokenAuthenticator()
    )

    controller_one = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=1.0,
        authenticator=authenticator,
        reconciliation_interval_seconds=60.0,
        database_path=str(database_path),
    )

    controller_one_task = asyncio.create_task(
        controller_one.start()
    )

    node_a = None
    node_b = None

    node_a_task = None
    node_b_task = None

    controller_two = None
    controller_two_task = None

    service_id = "controller-restart-service"

    command = (
        "python3 -c "
        "\"import time; time.sleep(60)\""
    )

    try:
        await wait_for_condition(
            lambda: controller_one.server is not None
        )

        assert controller_one.server is not None

        sockets = controller_one.server.sockets

        assert sockets

        controller_port = (
            sockets[0].getsockname()[1]
        )

        node_a = NodeAgent(
            node_id="controller-restart-node-a",
            hostname="controller-restart-host-a",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.1,
            heartbeat_interval_seconds=0.1,
        )

        node_b = NodeAgent(
            node_id="controller-restart-node-b",
            hostname="controller-restart-host-b",
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
                controller_one.registry.get_node(
                    "controller-restart-node-a"
                ).state
                == NodeState.ONLINE
                and controller_one.registry.get_node(
                    "controller-restart-node-b"
                ).state
                == NodeState.ONLINE
            )
        )

        await wait_for_condition(
            lambda: (
                controller_one.resource_registry.get_resources(
                    "controller-restart-node-a"
                )
                is not None
                and controller_one.resource_registry.get_resources(
                    "controller-restart-node-b"
                )
                is not None
            )
        )

        requirements = ServiceRequirements(
            cpu_cores=0.1,
            memory_mb=1,
            disk_gb=0.0,
        )

        intent = (
            controller_one.create_service_intent(
                service_id=service_id,
                command=command,
                requirements=requirements,
                desired_state=DesiredState.RUNNING,
            )
        )

        assert (
            intent.desired_state
            == DesiredState.RUNNING
        )

        start_response = (
            await controller_one.start_service(
                node_id="controller-restart-node-a",
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

        service_before = (
            controller_one.service_registry.get_service(
                service_id
            )
        )

        assert service_before is not None

        assert (
            service_before.node_id
            == "controller-restart-node-a"
        )

        assert service_before.pid == first_pid

        assert (
            service_before.status
            == "running"
        )

        persisted_intent = (
            controller_one.get_service_intent(
                service_id
            )
        )

        assert persisted_intent is not None

        assert (
            persisted_intent.desired_state
            == DesiredState.RUNNING
        )

        status_before = (
            await controller_one.status_service(
                node_id="controller-restart-node-a",
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

        # -----------------------------------------------------
        # REAL CONTROLLER RESTART
        # -----------------------------------------------------

        await controller_one.stop()

        # Controller.stop() closes the server, but the
        # serve_forever() task itself is cancelled here.
        if not controller_one_task.done():
            controller_one_task.cancel()

        try:
            await controller_one_task
        except asyncio.CancelledError:
            pass

        controller_one.close()

        controller_two = Controller(
            host="127.0.0.1",
            port=controller_port,
            heartbeat_timeout_seconds=1.0,
            authenticator=authenticator,
            reconciliation_interval_seconds=60.0,
            database_path=str(database_path),
        )

        controller_two_task = asyncio.create_task(
            controller_two.start()
        )

        await wait_for_condition(
            lambda: controller_two.server is not None
        )

        # The NodeAgents were NOT stopped.
        # They must reconnect to the new Controller.
        await wait_for_condition(
            lambda: (
                node_a.get_state()
                == AgentState.READY
                and node_b.get_state()
                == AgentState.READY
            ),
            timeout=15.0,
        )

        await wait_for_condition(
            lambda: (
                controller_two.registry.get_node(
                    "controller-restart-node-a"
                ).state
                == NodeState.ONLINE
                and controller_two.registry.get_node(
                    "controller-restart-node-b"
                ).state
                == NodeState.ONLINE
            ),
            timeout=15.0,
        )

        # Fresh resource reports must arrive after reconnect.
        await wait_for_condition(
            lambda: (
                controller_two.resource_registry.get_resources(
                    "controller-restart-node-a"
                )
                is not None
                and controller_two.resource_registry.get_resources(
                    "controller-restart-node-b"
                )
                is not None
            ),
            timeout=15.0,
        )

        # -----------------------------------------------------
        # VERIFY PERSISTED CONTROL-PLANE STATE
        # -----------------------------------------------------

        recovered_intent = (
            controller_two.get_service_intent(
                service_id
            )
        )

        assert recovered_intent is not None

        assert (
            recovered_intent.desired_state
            == DesiredState.RUNNING
        )

        assert (
            recovered_intent.command
            == command
        )

        assert (
            recovered_intent.requirements
            == requirements
        )

        recovered_service = (
            controller_two.service_registry.get_service(
                service_id
            )
        )

        assert recovered_service is not None

        assert (
            recovered_service.node_id
            == "controller-restart-node-a"
        )

        assert (
            recovered_service.command
            == command
        )

        assert (
            recovered_service.pid
            == first_pid
        )

        assert (
            recovered_service.status
            == "running"
        )

        # The actual OS service must still be alive on Node A.
        status_after = (
            await controller_two.status_service(
                node_id="controller-restart-node-a",
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
            == first_pid
        )

        # Reconciliation must recognize that the persisted
        # desired state is already satisfied.
        reconciliation = (
            await controller_two.reconcile_service(
                service_id
            )
        )

        assert (
            reconciliation.action
            == "none"
        )

        assert (
            reconciliation.changed
            is False
        )

        assert (
            reconciliation.reason
            == "already_running"
        )

        # The service must still belong to Node A.
        final_service = (
            controller_two.service_registry.get_service(
                service_id
            )
        )

        assert final_service is not None

        assert (
            final_service.node_id
            == "controller-restart-node-a"
        )

        assert (
            final_service.pid
            == first_pid
        )

        # Stop the recovered service cleanly.
        stop_response = (
            await controller_two.stop_service(
                node_id="controller-restart-node-a",
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

        if controller_two is not None:
            await controller_two.stop()

            if controller_two_task is not None:
                if not controller_two_task.done():
                    controller_two_task.cancel()

                try:
                    await controller_two_task
                except asyncio.CancelledError:
                    pass

            controller_two.close()

        else:
            if not controller_one_task.done():
                controller_one_task.cancel()

                try:
                    await controller_one_task
                except asyncio.CancelledError:
                    pass