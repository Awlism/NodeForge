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
async def test_recovery_control_loop_restarts_missing_runtime_service(
    monkeypatch,
):
    token = "d67-recovery-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
        authenticator=DevelopmentTokenAuthenticator(),
        reconciliation_interval_seconds=60.0,
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node = None
    node_task = None

    service_id = "d67-runtime-service"

    command = (
        "python3 -c "
        "\"import time; time.sleep(60)\""
    )

    try:
        assert await wait_for_condition(
            lambda: controller.server is not None
        )

        port = controller.server.sockets[0].getsockname()[1]

        node = NodeAgent(
            node_id="d67-node",
            hostname="d67-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        assert await wait_for_condition(
            lambda: node.get_state()
            == AgentState.READY
        )

        assert await wait_for_condition(
            lambda: (
                controller.registry.get_node(
                    "d67-node"
                )
                is not None
                and controller.registry.get_node(
                    "d67-node"
                ).state
                == NodeState.ONLINE
            )
        )

        controller.create_service_intent(
            service_id=service_id,
            command=command,
            desired_state=DesiredState.RUNNING,
        )

        started = await controller.start_service(
            node_id="d67-node",
            service_id=service_id,
            command=command,
        )

        assert (
            started.payload["status"]
            == "started"
        )

        service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert service is not None
        assert service.status == "running"

        # Remove the actual runtime service from the Node.
        await node._service_manager.stop_service(
            service_id
        )

        # Controller must observe the divergence.
        assert await wait_for_condition(
            lambda: (
                controller.service_registry
                .get_service(service_id)
                is not None
                and controller.service_registry
                .get_service(service_id).status
                == "stopped"
            ),
            timeout=6.0,
        )

        # Desired state is still RUNNING.
        assert (
            controller.get_service_intent(
                service_id
            ).desired_state
            == DesiredState.RUNNING
        )

        # Recovery control loop must start it again.
        result = await controller.reconcile_service(
            service_id
        )

        assert result.action == "start"
        assert result.changed is True

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

        recovered = (
            await controller.status_service(
                node_id="d67-node",
                service_id=service_id,
            )
        )

        assert (
            recovered.payload["status"]
            == "running"
        )

    finally:
        if node is not None:
            await node.stop()

        if node_task is not None:
            if not node_task.done():
                node_task.cancel()

            try:
                await node_task
            except asyncio.CancelledError:
                pass

        await controller.stop()

        if not controller_task.done():
            controller_task.cancel()

        try:
            await controller_task
        except asyncio.CancelledError:
            pass