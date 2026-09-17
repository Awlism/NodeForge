from freemesh.controller.resource_registry import ResourceRegistry
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


def test_register_and_get_resources():
    registry = ResourceRegistry()
    resources = make_resources()

    result = registry.register_resources(
        "node-1",
        resources,
    )

    assert result is resources
    assert registry.get_resources("node-1") is resources


def test_update_resources():
    registry = ResourceRegistry()

    registry.register_resources(
        "node-1",
        make_resources(cpu_usage=20.0),
    )

    updated = make_resources(
        cpu_usage=60.0,
        running_services=5,
    )

    result = registry.update_resources(
        "node-1",
        updated,
    )

    assert result is updated
    assert registry.get_resources(
        "node-1"
    ).cpu_usage_percent == 60.0

    assert registry.get_resources(
        "node-1"
    ).running_services == 5


def test_remove_resources():
    registry = ResourceRegistry()

    registry.register_resources(
        "node-1",
        make_resources(),
    )

    removed = registry.remove_resources("node-1")

    assert removed is not None
    assert registry.get_resources("node-1") is None
    assert registry.has_resources("node-1") is False


def test_list_resources_and_node_ids():
    registry = ResourceRegistry()

    registry.register_resources(
        "node-1",
        make_resources(),
    )

    registry.register_resources(
        "node-2",
        make_resources(cpu_usage=50.0),
    )

    assert registry.node_count() == 2
    assert set(registry.list_node_ids()) == {
        "node-1",
        "node-2",
    }

    resources = dict(registry.list_resources())

    assert "node-1" in resources
    assert "node-2" in resources


def test_missing_node_update_is_rejected():
    registry = ResourceRegistry()

    try:
        registry.update_resources(
            "node-1",
            make_resources(),
        )
        assert False
    except KeyError:
        pass


def test_invalid_node_id_is_rejected():
    registry = ResourceRegistry()

    try:
        registry.register_resources(
            "",
            make_resources(),
        )
        assert False
    except ValueError:
        pass


def test_invalid_resource_type_is_rejected():
    registry = ResourceRegistry()

    try:
        registry.register_resources(
            "node-1",
            object(),
        )
        assert False
    except TypeError:
        pass