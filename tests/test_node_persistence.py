"""Tests for persistent NodeForge node and resource state."""

from datetime import datetime, timezone

from freemesh.controller.node_registry import (
    NodeRegistry,
    NodeState,
)
from freemesh.controller.resource_registry import (
    ResourceRegistry,
)
from freemesh.node.resources import NodeResources


def make_resources(
    cpu_usage: float = 25.0,
    memory_used: int = 2048,
    disk_used: float = 40.0,
    running_services: int = 2,
) -> NodeResources:
    return NodeResources(
        cpu_cores=4,
        cpu_usage_percent=cpu_usage,
        memory_total_mb=8192,
        memory_used_mb=memory_used,
        disk_total_gb=100.0,
        disk_used_gb=disk_used,
        running_services=running_services,
    )


def test_node_registry_persists_node_state(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    registry_one = NodeRegistry(
        database_path=str(database_path)
    )

    node = registry_one.register_node(
        node_id="persistent-node",
        hostname="node-host",
        connection_address="127.0.0.1",
        connection_port=9001,
    )

    registry_one.authenticate_node(
        "persistent-node"
    )

    registry_one.record_heartbeat(
        "persistent-node"
    )

    assert node.state == NodeState.ONLINE
    assert node.authenticated is True
    assert node.last_heartbeat_time is not None

    registry_one.close()

    registry_two = NodeRegistry(
        database_path=str(database_path)
    )

    recovered = registry_two.get_node(
        "persistent-node"
    )

    assert recovered is not None
    assert recovered.node_id == "persistent-node"
    assert recovered.hostname == "node-host"
    assert recovered.connection_address == "127.0.0.1"
    assert recovered.connection_port == 9001
    assert recovered.state == NodeState.ONLINE
    assert recovered.authenticated is True
    assert recovered.last_heartbeat_time is not None

    registry_two.close()


def test_node_registry_persists_offline_state(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    registry_one = NodeRegistry(
        database_path=str(database_path)
    )

    registry_one.register_node(
        node_id="offline-node",
        hostname="offline-host",
    )

    registry_one.mark_offline(
        "offline-node"
    )

    registry_one.close()

    registry_two = NodeRegistry(
        database_path=str(database_path)
    )

    recovered = registry_two.get_node(
        "offline-node"
    )

    assert recovered is not None
    assert recovered.state == NodeState.OFFLINE

    registry_two.close()


def test_resource_registry_persists_resources(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    registry_one = ResourceRegistry(
        database_path=str(database_path)
    )

    resources = make_resources(
        cpu_usage=42.0,
        memory_used=3072,
        disk_used=55.0,
        running_services=4,
    )

    registry_one.register_resources(
        "persistent-node",
        resources,
    )

    registry_one.close()

    registry_two = ResourceRegistry(
        database_path=str(database_path)
    )

    recovered = registry_two.get_resources(
        "persistent-node"
    )

    assert recovered is not None
    assert recovered.cpu_cores == 4
    assert recovered.cpu_usage_percent == 42.0
    assert recovered.memory_total_mb == 8192
    assert recovered.memory_used_mb == 3072
    assert recovered.disk_total_gb == 100.0
    assert recovered.disk_used_gb == 55.0
    assert recovered.running_services == 4

    registry_two.close()


def test_resource_registry_updates_persisted_resources(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    registry_one = ResourceRegistry(
        database_path=str(database_path)
    )

    registry_one.register_resources(
        "node-1",
        make_resources(
            cpu_usage=20.0,
            running_services=1,
        ),
    )

    registry_one.update_resources(
        "node-1",
        make_resources(
            cpu_usage=70.0,
            memory_used=4096,
            running_services=6,
        ),
    )

    registry_one.close()

    registry_two = ResourceRegistry(
        database_path=str(database_path)
    )

    recovered = registry_two.get_resources(
        "node-1"
    )

    assert recovered is not None
    assert recovered.cpu_usage_percent == 70.0
    assert recovered.memory_used_mb == 4096
    assert recovered.running_services == 6

    registry_two.close()


def test_persistent_node_and_resources_share_database(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    node_registry = NodeRegistry(
        database_path=str(database_path)
    )

    resource_registry = ResourceRegistry(
        database_path=str(database_path)
    )

    node_registry.register_node(
        node_id="shared-node",
        hostname="shared-host",
        connection_address="10.0.0.5",
        connection_port=9001,
    )

    resource_registry.register_resources(
        "shared-node",
        make_resources(
            cpu_usage=35.0,
            running_services=3,
        ),
    )

    node_registry.close()
    resource_registry.close()

    recovered_nodes = NodeRegistry(
        database_path=str(database_path)
    )

    recovered_resources = ResourceRegistry(
        database_path=str(database_path)
    )

    node = recovered_nodes.get_node(
        "shared-node"
    )

    resources = recovered_resources.get_resources(
        "shared-node"
    )

    assert node is not None
    assert node.hostname == "shared-host"

    assert resources is not None
    assert resources.cpu_usage_percent == 35.0
    assert resources.running_services == 3

    recovered_nodes.close()
    recovered_resources.close()