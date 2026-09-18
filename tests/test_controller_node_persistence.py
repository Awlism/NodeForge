"""End-to-end tests for controller node/resource persistence."""

from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeState
from freemesh.node.resources import NodeResources


def make_resources() -> NodeResources:
    return NodeResources(
        cpu_cores=4,
        cpu_usage_percent=35.0,
        memory_total_mb=8192,
        memory_used_mb=2048,
        disk_total_gb=100.0,
        disk_used_gb=40.0,
        running_services=2,
    )


def test_controller_recovers_nodes_and_resources_after_restart(
    tmp_path,
):
    database_path = (
        tmp_path / "controller.db"
    )

    controller_one = Controller(
        database_path=str(database_path)
    )

    controller_one.registry.register_node(
        node_id="persistent-node",
        hostname="persistent-host",
        connection_address="127.0.0.1",
        connection_port=9001,
    )

    controller_one.registry.authenticate_node(
        "persistent-node"
    )

    controller_one.registry.record_heartbeat(
        "persistent-node"
    )

    controller_one.resource_registry.register_resources(
        "persistent-node",
        make_resources(),
    )

    node = controller_one.registry.get_node(
        "persistent-node"
    )

    resources = (
        controller_one.resource_registry.get_resources(
            "persistent-node"
        )
    )

    assert node is not None
    assert node.state == NodeState.ONLINE

    assert resources is not None
    assert resources.cpu_cores == 4
    assert resources.memory_used_mb == 2048
    assert resources.running_services == 2

    controller_one.close()

    controller_two = Controller(
        database_path=str(database_path)
    )

    recovered_node = (
        controller_two.registry.get_node(
            "persistent-node"
        )
    )

    recovered_resources = (
        controller_two.resource_registry.get_resources(
            "persistent-node"
        )
    )

    assert recovered_node is not None
    assert recovered_node.node_id == "persistent-node"
    assert recovered_node.hostname == "persistent-host"
    assert recovered_node.connection_address == "127.0.0.1"
    assert recovered_node.connection_port == 9001
    assert recovered_node.state == NodeState.ONLINE
    assert recovered_node.authenticated is True
    assert recovered_node.last_heartbeat_time is not None

    assert recovered_resources is not None
    assert recovered_resources.cpu_cores == 4
    assert recovered_resources.cpu_usage_percent == 35.0
    assert recovered_resources.memory_total_mb == 8192
    assert recovered_resources.memory_used_mb == 2048
    assert recovered_resources.disk_total_gb == 100.0
    assert recovered_resources.disk_used_gb == 40.0
    assert recovered_resources.running_services == 2

    controller_two.close()