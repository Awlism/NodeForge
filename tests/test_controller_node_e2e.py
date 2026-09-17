"""End-to-end tests for Controller <-> NodeAgent communication."""

import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_for_condition(
    condition,
    timeout: float = 5.0,
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


@pytest.mark.asyncio
async def test_controller_node_agent_end_to_end():
    """Test real Controller <-> NodeAgent service lifecycle."""

    token = "nodeforge-e2e-token"

    authenticator = DevelopmentTokenAuthenticator(
        token=token
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

    try:
        connected = await wait_for_condition(
            lambda: controller.server is not None,
        )

        assert connected is True

        assert controller.server is not None

        sockets = controller.server.sockets

        assert sockets

        port = sockets[0].getsockname()[1]

        node = NodeAgent(
            node_id="e2e-node",
            hostname="e2e-host",
            controller_host="127.0.0.1",
            controller_port=port,
            authentication_token=token,
            reconnect_delay_seconds=0.1,
            heartbeat_interval_seconds=0.1,
        )

        node_task = asyncio.create_task(
            node.start()
        )

        try:
            ready = await wait_for_condition(
                lambda: node.get_state()
                == AgentState.READY,
                timeout=5.0,
            )

            assert ready is True
            assert node.get_state() == AgentState.READY

            registered = await wait_for_condition(
                lambda: (
                    controller.registry.get_node(
                        "e2e-node"
                    )
                    is not None
                ),
                timeout=5.0,
            )

            assert registered is True

            node_info = controller.registry.get_node(
                "e2e-node"
            )

            assert node_info is not None
            assert node_info.authenticated is True
            assert node_info.state == NodeState.ONLINE

            resources_received = await wait_for_condition(
                lambda: (
                    controller.resource_registry
                    .get_resources("e2e-node")
                    is not None
                ),
                timeout=5.0,
            )

            assert resources_received is True

            resources = (
                controller.resource_registry.get_resources(
                    "e2e-node"
                )
            )

            assert resources is not None
            assert resources.cpu_cores > 0
            assert resources.memory_total_mb >= 0
            assert resources.disk_total_gb >= 0

            service_id = "e2e-service"

            start_response = (
                await controller.start_service(
                    node_id="e2e-node",
                    service_id=service_id,
                    command="python3 -c "
                    "\"import time; time.sleep(30)\"",
                )
            )

            assert (
                start_response.type.value
                == "service_start_response"
            )

            assert (
                start_response.payload["status"]
                == "started"
            )

            assert (
                start_response.payload["service_id"]
                == service_id
            )

            assert (
                start_response.payload["node_id"]
                == "e2e-node"
            )

            service = (
                controller.service_registry.get_service(
                    service_id
                )
            )

            assert service is not None
            assert service.node_id == "e2e-node"
            assert service.command == (
                "python3 -c "
                "\"import time; time.sleep(30)\""
            )

            status_response = (
                await controller.status_service(
                    node_id="e2e-node",
                    service_id=service_id,
                )
            )

            assert (
                status_response.payload["status"]
                == "running"
            )

            assert (
                status_response.payload["service_id"]
                == service_id
            )

            assert (
                status_response.payload["node_id"]
                == "e2e-node"
            )

            stop_response = (
                await controller.stop_service(
                    node_id="e2e-node",
                    service_id=service_id,
                )
            )

            assert (
                stop_response.payload["service_id"]
                == service_id
            )

            assert stop_response.payload["status"] in {
                "stopped",
                "success",
            }

            stopped_status = (
                await controller.status_service(
                    node_id="e2e-node",
                    service_id=service_id,
                )
            )

            assert (
                stopped_status.payload["status"]
                == "not_found"
            )

        finally:
            await node.stop()

            if not node_task.done():
                node_task.cancel()

            try:
                await node_task
            except asyncio.CancelledError:
                pass

    finally:
        await controller.stop()

        if not controller_task.done():
            controller_task.cancel()

        try:
            await controller_task
        except asyncio.CancelledError:
            pass