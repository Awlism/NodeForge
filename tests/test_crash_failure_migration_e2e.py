"""End-to-end test for crash recovery and automatic migration."""

import asyncio
import contextlib
import os
import tempfile

import pytest

from freemesh.controller.controller import Controller
from freemesh.node.agent import AgentState, NodeAgent
from freemesh.security.auth import DevelopmentTokenAuthenticator


async def wait_until(
    condition,
    timeout: float = 15.0,
    interval: float = 0.1,
) -> None:
    """Wait until an async condition becomes true."""

    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return

        await asyncio.sleep(interval)

    raise AssertionError(
        "Condition was not satisfied before timeout"
    )


@pytest.mark.asyncio
async def test_crash_failure_triggers_automatic_migration():
    """A permanently crashing service should migrate to another node."""

    token = "nodeforge-crash-migration-token"

    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=5.0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    controller_task = asyncio.create_task(
        controller.start()
    )

    node_a = None
    node_b = None

    counter_file = tempfile.NamedTemporaryFile(
        delete=False
    )

    counter_path = counter_file.name

    counter_file.close()

    try:
        await wait_until(
            lambda: (
                controller.server is not None
                and controller.server.sockets is not None
            ),
            timeout=5.0,
        )

        server_socket = (
            controller.server.sockets[0]
        )

        controller_port = (
            server_socket.getsockname()[1]
        )

        node_a = NodeAgent(
            node_id="crash-node-a",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.2,
            heartbeat_interval_seconds=0.2,
        )

        node_b = NodeAgent(
            node_id="migration-node-b",
            controller_host="127.0.0.1",
            controller_port=controller_port,
            authentication_token=token,
            reconnect_delay_seconds=0.2,
            heartbeat_interval_seconds=0.2,
        )

        node_a_task = asyncio.create_task(
            node_a.start()
        )

        node_b_task = asyncio.create_task(
            node_b.start()
        )

        try:
            await wait_until(
                lambda: (
                    node_a.get_state()
                    == AgentState.READY
                    and node_b.get_state()
                    == AgentState.READY
                ),
                timeout=10.0,
            )

            await wait_until(
                lambda: (
                    controller.registry.get_node(
                        "crash-node-a"
                    )
                    is not None
                    and controller.registry.get_node(
                        "migration-node-b"
                    )
                    is not None
                ),
                timeout=5.0,
            )

            await wait_until(
                lambda: (
                    controller.resource_registry.get_resources(
                        "crash-node-a"
                    )
                    is not None
                    and controller.resource_registry.get_resources(
                        "migration-node-b"
                    )
                    is not None
                ),
                timeout=5.0,
            )

            # The command intentionally fails exactly three
            # times globally. The fourth invocation stays alive.
            #
            # Initial start on Node A:
            #   invocation 1 -> crash
            #
            # RestartEngine on Node A:
            #   invocation 2 -> crash
            #   invocation 3 -> crash
            #   invocation 4 -> crash
            #
            # Migration to Node B:
            #   invocation 5 -> stays alive
            #
            # This lets the same command demonstrate both
            # restart exhaustion and successful migration.
            command = (
                "count_file="
                + counter_path
                + "; "
                "if [ -f \"$count_file\" ]; "
                "then count=$(cat \"$count_file\"); "
                "else count=0; fi; "
                "count=$((count + 1)); "
                "echo $count > \"$count_file\"; "
                "if [ \"$count\" -le 4 ]; "
                "then exit 1; "
                "else sleep 30; fi"
            )

            initial_response = (
                await controller.start_service(
                    node_id="crash-node-a",
                    service_id="crash-migration-service",
                    command=command,
                    timeout_seconds=10.0,
                )
            )

            assert (
                initial_response.payload.get(
                    "status"
                )
                == "started"
            )

            service = (
                controller.service_registry.get_service(
                    "crash-migration-service"
                )
            )

            assert service is not None

            assert (
                service.node_id
                == "crash-node-a"
            )

            initial_pid = service.pid

            assert initial_pid is not None

            # Wait until Node A has exhausted its restart
            # attempts and reports SERVICE_FAILURE.
            await wait_until(
                lambda: (
                    (
                        controller.service_registry.get_service(
                            "crash-migration-service"
                        )
                        is not None
                    )
                    and (
                        controller.service_registry.get_service(
                            "crash-migration-service"
                        ).restart_attempts
                        >= 3
                    )
                ),
                timeout=15.0,
            )

            # The Controller should eventually complete
            # automatic migration to Node B.
            await wait_until(
                lambda: (
                    (
                        controller.service_registry.get_service(
                            "crash-migration-service"
                        )
                        is not None
                    )
                    and (
                        controller.service_registry.get_service(
                            "crash-migration-service"
                        ).node_id
                        == "migration-node-b"
                    )
                    and (
                        controller.service_registry.get_service(
                            "crash-migration-service"
                        ).status
                        == "running"
                    )
                ),
                timeout=20.0,
            )

            migrated_service = (
                controller.service_registry.get_service(
                    "crash-migration-service"
                )
            )

            assert migrated_service is not None

            # The service must have moved from A to B.
            assert (
                migrated_service.node_id
                == "migration-node-b"
            )

            assert (
                migrated_service.status
                == "running"
            )

            # The final PID must be different from the
            # original process on Node A.
            assert migrated_service.pid is not None

            assert (
                migrated_service.pid
                != initial_pid
            )

            # Node A should have exhausted the restart
            # budget before migration.
            assert (
                migrated_service.restart_attempts
                >= 3
            )

            # The migration should have created a valid
            # resource reservation on Node B.
            reservation = (
                controller.resource_accounting.get(
                    "crash-migration-service"
                )
            )

            assert reservation is not None

            assert (
                reservation.node_id
                == "migration-node-b"
            )

            # Verify that Node B really has the service.
            node_b_service = (
                node_b._service_manager.get_service(
                    "crash-migration-service"
                )
            )

            assert node_b_service is not None

            assert (
                node_b_service.status.value
                == "running"
            )

            assert (
                node_b_service.pid
                == migrated_service.pid
            )

            # The process must actually still be alive.
            node_b_process = (
                node_b._service_manager.get_process(
                    "crash-migration-service"
                )
            )

            assert node_b_process is not None

            assert (
                node_b_process.returncode
                is None
            )

            # The shared counter must show that the
            # service was started four times on A and
            # once on B.
            with open(
                counter_path,
                "r",
                encoding="utf-8",
            ) as file:
                invocation_count = int(
                    file.read().strip()
                )

            assert invocation_count >= 5

        finally:
            if node_a is not None:
                await node_a.stop()

            if node_b is not None:
                await node_b.stop()

            for task in (
                locals().get("node_a_task"),
                locals().get("node_b_task"),
            ):
                if task is not None:
                    task.cancel()

                    with contextlib.suppress(
                        asyncio.CancelledError
                    ):
                        await task

    finally:
        await controller.stop()

        controller_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await controller_task

        try:
            os.unlink(counter_path)
        except FileNotFoundError:
            pass