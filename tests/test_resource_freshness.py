import time

from freemesh.controller.resource_registry import (
    ResourceRegistry,
)
from freemesh.node.resources import NodeResources


def make_resources() -> NodeResources:
    return NodeResources(
        cpu_cores=4.0,
        cpu_usage_percent=20.0,
        memory_total_mb=8192,
        memory_used_mb=2048,
        disk_total_gb=100.0,
        disk_used_gb=25.0,
        running_services=1,
    )


def test_resource_report_is_fresh_immediately():
    registry = ResourceRegistry(
        freshness_timeout_seconds=30.0,
    )

    registry.register_resources(
        "node-1",
        make_resources(),
    )

    assert registry.get_resources("node-1") is not None

    registry.close()


def test_stale_resource_is_not_returned_for_scheduling():
    registry = ResourceRegistry(
        freshness_timeout_seconds=0.05,
    )

    registry.register_resources(
        "node-1",
        make_resources(),
    )

    assert registry.get_resources("node-1") is not None

    time.sleep(0.1)

    assert registry.get_resources("node-1") is None

    registry.close()


def test_stale_resource_can_still_be_read_explicitly():
    registry = ResourceRegistry(
        freshness_timeout_seconds=0.05,
    )

    resources = make_resources()

    registry.register_resources(
        "node-1",
        resources,
    )

    time.sleep(0.1)

    assert registry.get_resources(
        "node-1",
        allow_stale=True,
    ) == resources

    registry.close()


def test_fresh_report_restores_node_resources():
    registry = ResourceRegistry(
        freshness_timeout_seconds=0.05,
    )

    first_resources = make_resources()

    registry.register_resources(
        "node-1",
        first_resources,
    )

    time.sleep(0.1)

    assert registry.get_resources("node-1") is None

    updated_resources = NodeResources(
        cpu_cores=8.0,
        cpu_usage_percent=10.0,
        memory_total_mb=16384,
        memory_used_mb=1024,
        disk_total_gb=200.0,
        disk_used_gb=20.0,
        running_services=0,
    )

    registry.update_resources(
        "node-1",
        updated_resources,
    )

    assert registry.get_resources(
        "node-1"
    ) == updated_resources

    registry.close()


def test_resource_freshness_survives_controller_restart(tmp_path):
    database_path = str(
        tmp_path / "nodeforge.db"
    )

    registry = ResourceRegistry(
        database_path=database_path,
        freshness_timeout_seconds=30.0,
    )

    resources = make_resources()

    registry.register_resources(
        "node-1",
        resources,
    )

    registry.close()

    recovered_registry = ResourceRegistry(
        database_path=database_path,
        freshness_timeout_seconds=30.0,
    )

    assert recovered_registry.get_resources(
        "node-1"
    ) == resources

    recovered_registry.close()


def test_stale_persisted_resource_is_not_schedulable(
    tmp_path,
):
    database_path = str(
        tmp_path / "nodeforge.db"
    )

    registry = ResourceRegistry(
        database_path=database_path,
        freshness_timeout_seconds=0.05,
    )

    registry.register_resources(
        "node-1",
        make_resources(),
    )

    registry.close()

    time.sleep(0.1)

    recovered_registry = ResourceRegistry(
        database_path=database_path,
        freshness_timeout_seconds=0.05,
    )

    assert recovered_registry.get_resources(
        "node-1"
    ) is None

    assert recovered_registry.get_resources(
        "node-1",
        allow_stale=True,
    ) is not None

    recovered_registry.close()