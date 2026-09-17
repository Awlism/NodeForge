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
    """Wait until a synchronous condition becomes true."""

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

    node_a_task = None
    node_b_task = None

    counter_file = tempfile.NamedTemporaryFile(
        delete=False
    )

    counter_path = counter_file.name

    counter_file.close()

    try:
        # -------------------------------------------------
        # Start Controller
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Create Nodes
        # -------------------------------------------------

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
            # -------------------------------------------------
            # Wait for both Nodes to become READY
            # -------------------------------------------------

            await wait_until(
                lambda: (
                    node_a.get_state()
                    == AgentState.READY
                    and node_b.get_state()
                    == AgentState.READY
                ),
                timeout=10.0,
            )

            # -------------------------------------------------
            # Wait for Controller registrations
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Wait for resource reports
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Crash command
            # -------------------------------------------------
            #
            # Invocation 1:
            #   Initial service start on Node A -> crash
            #
            # Invocation 2:
            #   Restart attempt 1 -> crash
            #
            # Invocation 3:
            #   Restart attempt 2 -> crash
            #
            # Invocation 4:
            #   Restart attempt 3 -> crash
            #
            # Invocation 5:
            #   Migration to Node B -> stays alive
            #
            # Therefore:
            #
            #   Node A = 4 failed processes
            #   Node B = 1 healthy process
            #
            # -------------------------------------------------

            command = (
                "count_file="
                + counter_path
                + "; "
                "if [ -f \"$count_file\" ]; "
                "then "
                "count=$(cat \"$count_file\"); "
                "else "
                "count=0; "
                "fi; "
                "count=$((count + 1)); "
                "echo $count > \"$count_file\"; "
                "if [ \"$count\" -le 4 ]; "
                "then "
                "exit 1; "
                "else "
                "sleep 30; "
                "fi"
            )

            # -------------------------------------------------
            # Start service on Node A
            # -------------------------------------------------

            initial_response = (
                await controller.start_service(
                    node_id="crash-node-a",
                    service_id=(
                        "crash-migration-service"
                    ),
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

            # -------------------------------------------------
            # Verify initial Controller registry state
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Verify Node A has the actual Service model
            # -------------------------------------------------

            node_a_service = (
                node_a._service_manager.get_service(
                    "crash-migration-service"
                )
            )

            assert node_a_service is not None

            assert (
                node_a_service.restart_attempts
                == 0
            )

            # -------------------------------------------------
            # Wait for RestartEngine exhaustion
            # -------------------------------------------------
            #
            # IMPORTANT:
            # restart_attempts belongs to the Service model
            # running inside NodeAgent, not ServiceInfo inside
            # Controller.service_registry.
            # -------------------------------------------------

            await wait_until(
                lambda: (
                    (
                        node_a._service_manager.get_service(
                            "crash-migration-service"
                        )
                        is not None
                    )
                    and (
                        node_a._service_manager.get_service(
                            "crash-migration-service"
                        ).restart_attempts
                        >= 3
                    )
                ),
                timeout=15.0,
            )

            # -------------------------------------------------
            # Verify exactly three restart attempts
            # -------------------------------------------------

            node_a_service = (
                node_a._service_manager.get_service(
                    "crash-migration-service"
                )
            )

            assert node_a_service is not None

            assert (
                node_a_service.restart_attempts
                == 3
            )

            # The NodeAgent should have exhausted the
            # restart budget and marked the service crashed.
            assert (
                node_a_service.status.value
                == "crashed"
            )

            # -------------------------------------------------
            # Verify Controller received the failure
            # -------------------------------------------------

            await wait_until(
                lambda: (
                    controller.failure_manager
                    .get_failure(
                        "crash-migration-service"
                    )
                    is not None
                ),
                timeout=10.0,
            )

            failure = (
                controller.failure_manager.get_failure(
                    "crash-migration-service"
                )
            )

            assert failure is not None

            assert (
                failure.node_id
                == "crash-node-a"
            )

            assert (
                failure.status
                == "crashed"
            )

            assert (
                failure.restart_attempts
                == 3
            )

            # -------------------------------------------------
            # Wait for automatic migration to Node B
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Read final Controller service state
            # -------------------------------------------------

            migrated_service = (
                controller.service_registry.get_service(
                    "crash-migration-service"
                )
            )

            assert migrated_service is not None

            # Service must have moved to Node B.
            assert (
                migrated_service.node_id
                == "migration-node-b"
            )

            assert (
                migrated_service.status
                == "running"
            )

            # A new process must have been created.
            assert migrated_service.pid is not None

            assert (
                migrated_service.pid
                != initial_pid
            )

            # -------------------------------------------------
            # Verify migration registry
            # -------------------------------------------------

            migration = (
                controller.migration_registry.get(
                    "crash-migration-service"
                )
            )

            assert migration is not None

            assert (
                migration.status
                == "migrated"
            )

            assert (
                migration.source_node_id
                == "crash-node-a"
            )

            assert (
                migration.target_node_id
                == "migration-node-b"
            )

            # -------------------------------------------------
            # Verify resource accounting
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Verify Node B Service model
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Verify Node B actual process
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Verify the shared command counter
            # -------------------------------------------------

            with open(
                counter_path,
                "r",
                encoding="utf-8",
            ) as file:
                invocation_count = int(
                    file.read().strip()
                )

            # Four failed executions on Node A
            # plus one successful execution on Node B.
            assert invocation_count == 5

        finally:
            # -------------------------------------------------
            # Stop Nodes
            # -------------------------------------------------

            if node_a is not None:
                await node_a.stop()

            if node_b is not None:
                await node_b.stop()

            # -------------------------------------------------
            # Cancel Node tasks
            # -------------------------------------------------

            for task in (
                node_a_task,
                node_b_task,
            ):
                if task is not None:
                    task.cancel()

                    with contextlib.suppress(
                        asyncio.CancelledError
                    ):
                        await task

        finally:
            pass

    finally:
        # -----------------------------------------------------
        # Stop Controller
        # -----------------------------------------------------

        await controller.stop()

        controller_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await controller_task

        # -----------------------------------------------------
        # Remove temporary counter file
        # -----------------------------------------------------

        try:
            os.unlink(counter_path)
        except FileNotFoundError:
            pass