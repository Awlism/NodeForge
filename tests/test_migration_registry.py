from freemesh.controller.migration_registry import (
    MigrationRegistry,
)


def test_start_migration():
    registry = MigrationRegistry()

    record = registry.start(
        service_id="service-1",
        source_node_id="node-a",
        target_node_id="node-b",
    )

    assert record.service_id == "service-1"
    assert record.source_node_id == "node-a"
    assert record.target_node_id == "node-b"
    assert record.status == "started"


def test_complete_migration():
    registry = MigrationRegistry()

    registry.start(
        "service-1",
        "node-a",
        "node-b",
    )

    record = registry.complete(
        "service-1",
        pid=1234,
    )

    assert record.status == "migrated"
    assert record.pid == 1234
    assert record.error is None


def test_failed_migration():
    registry = MigrationRegistry()

    registry.start(
        "service-1",
        "node-a",
        "node-b",
    )

    record = registry.fail(
        "service-1",
        "target unavailable",
    )

    assert record.status == "failed"
    assert record.error == "target unavailable"


def test_verification_failure():
    registry = MigrationRegistry()

    registry.start(
        "service-1",
        "node-a",
        "node-b",
    )

    record = registry.fail(
        "service-1",
        "health check failed",
        status="verification_failed",
    )

    assert record.status == "verification_failed"


def test_list_records():
    registry = MigrationRegistry()

    registry.start(
        "service-1",
        "node-a",
        "node-b",
    )

    registry.start(
        "service-2",
        "node-a",
        "node-c",
    )

    assert len(
        registry.list_records()
    ) == 2


def test_clear_migration():
    registry = MigrationRegistry()

    registry.start(
        "service-1",
        "node-a",
        "node-b",
    )

    registry.clear(
        "service-1"
    )

    assert registry.get(
        "service-1"
    ) is None