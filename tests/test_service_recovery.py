import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.controller.service_intent import DesiredState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_for_condition(
    condition,
    timeout: float = 8.0,
    interval: float = 0.05,
) -> bool:
    """Wait until an asynchronous condition becomes true."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        if condition():
            return True

        await asyncio.sleep(interval)

    return condition()


async def wait_for_node(
    controller: Controller,
    node_id: str,
    state: NodeState,
    timeout: float = 8.0,
):
    """Wait until a node reaches the requested state."""

    result = None

    async def condition():
        nonlocal result

        result = controller.registry.get_node(node_id)

        return (
            result is not None
            and result.state == state
        )

    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        if await condition():
            return result

        await asyncio.sleep(0.05)

    return result


@pytest.mark.asyncio
async def test_service_metadata_survives_node_disconnect(
    monkeypatch,
):
    """A running service remains recoverable after its node disconnects."""

    token = "d64-metadata-token"

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

    try:
        started = await wait_for_condition(
            lambda: controller.server is not None,
        )

        assert started is True
        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        node = NodeAgent(
            node_id="d64-recovery-node",
            hostname="d64-recovery-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        ready = await wait_for_condition(
            lambda: (
                node.get_state()
                == AgentState.READY
            ),
        )

        assert ready is True

        node_info = await wait_for_node(
            controller,
            "d64-recovery-node",
            NodeState.ONLINE,
        )

        assert node_info is not None
        assert node_info.authenticated is True

        service_id = "d64-service"

        command = (
            "python3 -c "
            "\"import time; time.sleep(60)\""
        )

        response = await controller.start_service(
            node_id="d64-recovery-node",
            service_id=service_id,
            command=command,
        )

        assert (
            response.payload["status"]
            == "started"
        )

        service = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert service is not None
        assert service.node_id == "d64-recovery-node"
        assert service.pid is not None
        assert service.command == command

        await node.transport.disconnect()

        await asyncio.sleep(0.2)

        recovered_metadata = (
            controller.service_registry.get_service(
                service_id
            )
        )

        assert recovered_metadata is not None
        assert (
            recovered_metadata.node_id
            == "d64-recovery-node"
        )
        assert (
            recovered_metadata.command
            == command
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


@pytest.mark.asyncio
async def test_desired_running_service_remains_recoverable_after_node_loss(
    monkeypatch,
):
    """A RUNNING desired state remains persisted after node loss."""

    token = "d64-intent-token"

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

    try:
        started = await wait_for_condition(
            lambda: controller.server is not None,
        )

        assert started is True
        assert controller.server is not None

        port = controller.server.sockets[0].getsockname()[1]

        node = NodeAgent(
            node_id="d64-intent-node",
            hostname="d64-intent-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            heartbeat_interval_seconds=0.1,
            reconnect_delay_seconds=0.1,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        ready = await wait_for_condition(
            lambda: (
                node.get_state()
                == AgentState.READY
            ),
        )

        assert ready is True

        node_info = await wait_for_node(
            controller,
            "d64-intent-node",
            NodeState.ONLINE,
        )

        assert node_info is not None

        service_id = "d64-intent-service"

        command = (
            "python3 -c "
            "\"import time; time.sleep(60)\""
        )

        intent = controller.create_service_intent(
            service_id=service_id,
            command=command,
            desired_state=DesiredState.RUNNING,
        )

        assert (
            intent.desired_state
            == DesiredState.RUNNING
        )

        start_response = await controller.start_service(
            node_id="d64-intent-node",
            service_id=service_id,
            command=command,
        )

        assert (
            start_response.payload["status"]
            == "started"
        )

        await node.transport.disconnect()

        await asyncio.sleep(0.2)

        stored_intent = (
            controller.get_service_intent(
                service_id
            )
        )

        assert stored_intent is not None
        assert (
            stored_intent.desired_state
            == DesiredState.RUNNING
        )
        assert (
            stored_intent.command
            == command
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