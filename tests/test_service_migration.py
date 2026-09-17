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
        pid=1234,
        error=None,
    ):
        self.payload = {
            "status": status,
            "pid": pid,
        }

        if error:
            self.payload["error"] = error


@pytest.mark.asyncio
async def test_migration_manager_moves_service():
    manager = MigrationManager()

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=5,
    )

    manager.reserve_service(
        service_id="service-1",
        node_id="node-a",
        requirements=requirements,
    )

    plan = MigrationPlan(
        service_id="service-1",
        source_node_id="node-a",
        target_node_id="node-b",
        command="python3 bot.py",
        requirements=requirements,
    )

    calls = []

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        calls.append(
            {
                "node_id": node_id,
                "service_id": service_id,
                "command": command,
                "requirements": requirements,
            }
        )

        return FakeResponse(
            status="started",
            pid=9001,
        )

    result = await manager.execute(
        plan=plan,
        start_service=start_service,
    )

    assert result.status == "migrated"
    assert result.target_node_id == "node-b"
    assert result.pid == 9001

    assert calls[0]["node_id"] == "node-b"
    assert calls[0]["service_id"] == "service-1"

    reservation = manager.accounting.get(
        "service-1"
    )

    assert reservation is not None
    assert reservation.node_id == "node-b"


@pytest.mark.asyncio
async def test_migration_fails_without_target_start():
    manager = MigrationManager()

    requirements = ServiceRequirements(
        cpu_cores=1,
        memory_mb=512,
        disk_gb=1,
    )

    plan = MigrationPlan(
        service_id="service-1",
        source_node_id="node-a",
        target_node_id="node-b",
        command="python3 bot.py",
        requirements=requirements,
    )

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        return FakeResponse(
            status="failed",
            error="insufficient resources",
        )

    result = await manager.execute(
        plan=plan,
        start_service=start_service,
    )

    assert result.status == "failed"
    assert result.target_node_id == "node-b"
    assert result.error == "insufficient resources"

    assert (
        manager.accounting.get(
            "service-1"
        )
        is None
    )


@pytest.mark.asyncio
async def test_migration_failure_is_captured():
    manager = MigrationManager()

    requirements = ServiceRequirements(
        cpu_cores=1,
        memory_mb=512,
        disk_gb=1,
    )

    plan = MigrationPlan(
        service_id="service-1",
        source_node_id="node-a",
        target_node_id="node-b",
        command="python3 bot.py",
        requirements=requirements,
    )

    async def start_service(
        node_id,
        service_id,
        command,
        requirements,
    ):
        raise RuntimeError(
            "node connection lost"
        )

    result = await manager.execute(
        plan=plan,
        start_service=start_service,
    )

    assert result.status == "failed"
    assert result.error == "node connection lost"