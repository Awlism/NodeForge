import pytest

from freemesh.controller.migration_manager import (
    MigrationManager,
)
from freemesh.controller.resource_failover import (
    MigrationPlan,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


class FakeResponse:
    def __init__(
        self,
        status="started",
        pid=1000,
        error=None,
    ):
        self.payload = {
            "status": status,
            "pid": pid,
        }

        if error:
            self.payload["error"] = error


def make_plan():
    return MigrationPlan(
        service_id="service-1",
        source_node_id="node-a",
        target_node_id="node-b",
        command="python3 bot.py",
        requirements=ServiceRequirements(
            cpu_cores=2,
            memory_mb=2048,
            disk_gb=5,
        ),
    )


@pytest.mark.asyncio
async def test_full_migration_start_verify_commit():
    manager = MigrationManager()

    manager.reserve_service(
        "service-1",
        "node-a",
        make_plan().requirements,
    )

    calls = []

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        calls.append(
            (
                "start",
                node_id,
                service_id,
            )
        )

        return FakeResponse(
            status="started",
            pid=5555,
        )

    async def verify_service(
        node_id,
        service_id,
    ):
        calls.append(
            (
                "verify",
                node_id,
                service_id,
            )
        )

        return True

    result = await manager.execute(
        make_plan(),
        start_service,
        verify_service,
    )

    assert result.status == "migrated"
    assert result.pid == 5555

    assert calls == [
        (
            "start",
            "node-b",
            "service-1",
        ),
        (
            "verify",
            "node-b",
            "service-1",
        ),
    ]

    reservation = manager.accounting.get(
        "service-1"
    )

    assert reservation is not None
    assert reservation.node_id == "node-b"


@pytest.mark.asyncio
async def test_migration_verification_failure_does_not_commit():
    manager = MigrationManager()

    manager.reserve_service(
        "service-1",
        "node-a",
        make_plan().requirements,
    )

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        return FakeResponse(
            status="started",
            pid=7777,
        )

    async def verify_service(
        node_id,
        service_id,
    ):
        return False

    result = await manager.execute(
        make_plan(),
        start_service,
        verify_service,
    )

    assert result.status == (
        "verification_failed"
    )

    reservation = manager.accounting.get(
        "service-1"
    )

    assert reservation is not None
    assert reservation.node_id == "node-a"


@pytest.mark.asyncio
async def test_migration_start_failure_keeps_source_reservation():
    manager = MigrationManager()

    manager.reserve_service(
        "service-1",
        "node-a",
        make_plan().requirements,
    )

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        return FakeResponse(
            status="failed",
            error="node rejected service",
        )

    result = await manager.execute(
        make_plan(),
        start_service,
    )

    assert result.status == "failed"

    reservation = manager.accounting.get(
        "service-1"
    )

    assert reservation is not None
    assert reservation.node_id == "node-a"


@pytest.mark.asyncio
async def test_migration_exception_is_captured():
    manager = MigrationManager()

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        raise RuntimeError(
            "connection lost"
        )

    result = await manager.execute(
        make_plan(),
        start_service,
    )

    assert result.status == "failed"
    assert result.error == "connection lost"


@pytest.mark.asyncio
async def test_duplicate_migration_is_rejected():
    manager = MigrationManager()

    manager._active_migrations.add(
        "service-1"
    )

    result = await manager.execute(
        make_plan(),
        lambda **kwargs: None,
    )

    assert result.status == (
        "already_migrating"
    )