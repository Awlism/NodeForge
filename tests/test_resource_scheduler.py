from freemesh.node.resources import NodeResources
from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
    ResourceScheduler,
)


def make_resources(
    cpu_usage: float,
    memory_used: int,
    disk_used: float = 20.0,
) -> NodeResources:
    return NodeResources(
        cpu_cores=4,
        cpu_usage_percent=cpu_usage,
        memory_total_mb=8192,
        memory_used_mb=memory_used,
        disk_total_gb=100.0,
        disk_used_gb=disk_used,
    )


def test_selects_node_with_enough_resources():
    scheduler = ResourceScheduler()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        resources=make_resources(
            cpu_usage=80.0,
            memory_used=7000,
        ),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        resources=make_resources(
            cpu_usage=20.0,
            memory_used=2000,
        ),
    )

    selected = scheduler.select_node(
        [node_a, node_b],
        required_cpu_cores=1.0,
        required_memory_mb=1024,
        required_disk_gb=5.0,
    )

    assert selected is not None
    assert selected.node_id == "node-b"


def test_ignores_node_without_capacity():
    scheduler = ResourceScheduler()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        resources=make_resources(
            cpu_usage=95.0,
            memory_used=7800,
        ),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        resources=make_resources(
            cpu_usage=30.0,
            memory_used=3000,
        ),
    )

    selected = scheduler.select_node(
        [node_a, node_b],
        required_cpu_cores=1.0,
        required_memory_mb=1024,
        required_disk_gb=5.0,
    )

    assert selected is not None
    assert selected.node_id == "node-b"


def test_ignores_unavailable_nodes():
    scheduler = ResourceScheduler()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        available=False,
        resources=make_resources(
            cpu_usage=10.0,
            memory_used=1000,
        ),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        resources=make_resources(
            cpu_usage=40.0,
            memory_used=3000,
        ),
    )

    selected = scheduler.select_node(
        [node_a, node_b],
        required_memory_mb=512,
    )

    assert selected is not None
    assert selected.node_id == "node-b"


def test_can_exclude_failed_node():
    scheduler = ResourceScheduler()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        resources=make_resources(
            cpu_usage=10.0,
            memory_used=1000,
        ),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        resources=make_resources(
            cpu_usage=30.0,
            memory_used=3000,
        ),
    )

    selected = scheduler.select_node(
        [node_a, node_b],
        required_memory_mb=512,
        exclude_node_id="node-a",
    )

    assert selected is not None
    assert selected.node_id == "node-b"


def test_returns_none_when_no_node_has_capacity():
    scheduler = ResourceScheduler()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        resources=make_resources(
            cpu_usage=95.0,
            memory_used=8000,
            disk_used=98.0,
        ),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        resources=make_resources(
            cpu_usage=95.0,
            memory_used=8000,
            disk_used=98.0,
        ),
    )

    selected = scheduler.select_node(
        [node_a, node_b],
        required_cpu_cores=2.0,
        required_memory_mb=2048,
        required_disk_gb=10.0,
    )

    assert selected is None