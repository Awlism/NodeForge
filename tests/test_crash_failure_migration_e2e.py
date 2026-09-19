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


def print_migration_diagnostic(
    controller: Controller,
    service_id: str,
    node_a: NodeAgent | None,
    node_b: NodeAgent | None,
) -> None:
    """Print detailed migration state after a timeout."""

    print("\n")
    print("=" * 72)
    print("NODEFORGE MIGRATION DIAGNOSTIC")
    print("=" * 72)

    migration = controller.migration_registry.get(
        service_id
    )

    if migration is None:
        print("Migration record: NONE")
    else:
        print("Migration record:")
        print(
            f"  status       = {migration.status}"
        )
        print(
            f"  source_node  = {migration.source_node_id}"
        )
        print(
            f"  target_node  = {migration.target_node_id}"
        )
        print(
            f"  pid          = {migration.pid}"
        )
        print(
            f"  error        = {migration.error!r}"
        )

    service = (
        controller.service_registry.get_service(
            service_id
        )
    )

    if service is None:
        print("Canonical service: NONE")
    else:
        print("Canonical service:")
        print(
            f"  node_id      = {service.node_id}"
        )
        print(
            f"  status       = {service.status}"
        )
        print(
            f"  pid          = {service.pid}"
        )
        print(
            "  restart      = unavailable "
            "(canonical ServiceInfo does not track "
            "restart attempts)"
        )

    failure = (
        controller.failure_manager.get_failure(
            service_id
        )
    )

    if failure is None:
        print("Failure record: NONE")
    else:
        print("Failure record:")
        print(
            f"  node_id      = {failure.node_id}"
        )
        print(
            f"  status       = {failure.status}"
        )
        print(
            f"  restart      = {failure.restart_attempts}"
        )
        print(
            f"  reason       = {failure.reason!r}"
        )

    reservation = (
        controller.resource_accounting.get(
            service_id
        )
    )

    if reservation is None:
        print("Resource reservation: NONE")
    else:
        print("Resource reservation:")
        print(
            f"  node_id      = {reservation.node_id}"
        )

    for label, node in (
        ("Node A", node_a),
        ("Node B", node_b),
    ):
        if node is None:
            print(f"{label}: NONE")
            continue

        print(f"{label}:")
        print(
            f"  state        = {node.get_state()}"
        )

        try:
            node_service = (
                node._service_manager.get_service(
                    service_id
                )
            )
        except Exception as exc:
            print(
                f"  service read = ERROR: {exc!r}"
            )
            continue

        if node_service is None:
            print("  service      = NONE")
        else:
            print(
                f"  service.node = {node_service.node_id}"
            )
            print(
                f"  service.stat = {node_service.status}"
            )
            print(
                f"  service.pid  = {node_service.pid}"
            )
            print(
                f"  restart      = "
                f"{node_service.restart_attempts}"
            )

            try:
                process = (
                    node._service_manager.get_process(
                        service_id
                    )
                )
            except Exception as exc:
                print(
                    f"  process read = ERROR: {exc!r}"
                )
                process = None

            if process is None:
                print("  process      = NONE")
            else:
                print(
                    f"  process.pid  = {process.pid}"
                )
                print(
                    f"  returncode   = "
                    f"{process.returncode}"
                )

    print("=" * 72)
    print("END MIGRATION DIAGNOSTIC")
    print("=" * 72)
    print()


@pytest.mark.asyncio
async def test_crash_failure_triggers_automatic_migration(
    monkeypatch,
):
    """A crashing service should migrate after restart exhaustion."""

    token = "nodeforge-crash-migration-token"

    monkeypatch.setenv(
        "NODEFORGE_AUTH_TOKEN",
        token,
    )

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
        await wait_until(
            lambda: (
                controller.server is not None
                and controller.server.sockets is not None
            ),
            timeout=5.0,
        )

        controller_port = (
            controller.server.sockets[0]
            .getsockname()[1]
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

            response = await controller.start_service(
                node_id="crash-node-a",
                service_id="crash-migration-service",
                command=command,
                timeout_seconds=10.0,
            )

            assert (
                response.payload.get("status")
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

            await wait_until(
                lambda: (
                    controller.failure_manager.get_failure(
                        "crash-migration-service"
                    )
                    is not None
                ),
                timeout=30.0,
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

            service_id = (
                "crash-migration-service"
            )

            try:
                await wait_until(
                    lambda: (
                        (
                            controller.service_registry.get_service(
                                service_id
                            )
                            is not None
                        )
                        and (
                            controller.service_registry.get_service(
                                service_id
                            ).node_id
                            == "migration-node-b"
                        )
                        and (
                            controller.service_registry.get_service(
                                service_id
                            ).status
                            == "running"
                        )
                    ),
                    timeout=30.0,
                )

            except AssertionError:
                print_migration_diagnostic(
                    controller=controller,
                    service_id=service_id,
                    node_a=node_a,
                    node_b=node_b,
                )
                raise

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
                migrated_service.status
                == "running"
            )

            assert migrated_service.pid is not None

            assert (
                migrated_service.pid
                != initial_pid
            )

            migration = (
                controller.migration_registry.get(
                    service_id
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

            reservation = (
                controller.resource_accounting.get(
                    service_id
                )
            )

            assert reservation is not None

            assert (
                reservation.node_id
                == "migration-node-b"
            )

            node_b_service = (
                node_b._service_manager.get_service(
                    service_id
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

            node_b_process = (
                node_b._service_manager.get_process(
                    service_id
                )
            )

            assert node_b_process is not None

            assert (
                node_b_process.returncode
                is None
            )

            # The source runtime must have been fenced and
            # removed from Node A before migration committed.
            assert (
                node_a._service_manager.get_service(
                    service_id
                )
                is None
            )

            assert (
                node_a._service_manager.get_process(
                    service_id
                )
                is None
            )

            with open(
                counter_path,
                "r",
                encoding="utf-8",
            ) as file:
                invocation_count = int(
                    file.read().strip()
                )

            assert invocation_count == 5

        finally:
            if node_a is not None:
                await node_a.stop()

            if node_b is not None:
                await node_b.stop()

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