import pytest

from freemesh.controller.service_placement import (
    PlacementResult,
    ServicePlacement,
)
from freemesh.node.resources import NodeResources
from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
)
from freemesh.service_requirements import ServiceRequirements


def make_resources(
    cpu_cores=8.0,
    cpu_usage_percent=20.0,
    memory_total_mb=16384,
    memory_used_mb=2048,
    disk_total_gb=200.0,
    disk_used_gb=20.0,
):
    return NodeResources(
        cpu_cores=cpu_cores,
        cpu_usage_percent=cpu_usage_percent,
        memory_total_mb=memory_total_mb,
        memory_used_mb=memory_used_mb,
        disk_total_gb=disk_total_gb,
        disk_used_gb=disk_used_gb,
    )


def test_service_placement_selects_suitable_node():
    placement = ServicePlacement()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(),
    )

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=10,
    )

    result = placement.select_node(
        service_id="service-1",
        requirements=requirements,
        nodes=[node],
    )

    assert isinstance(result, PlacementResult)
    assert result.service_id == "service-1"
    assert result.node_id == "node-a"
    assert result.requirements == requirements


def test_service_placement_rejects_insufficient_capacity():
    placement = ServicePlacement()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(
            cpu_cores=2,
            cpu_usage_percent=80,
            memory_total_mb=2048,
            memory_used_mb=1900,
            disk_total_gb=10,
            disk_used_gb=9,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=4096,
        disk_gb=20,
    )

    result = placement.select_node(
        service_id="service-1",
        requirements=requirements,
        nodes=[node],
    )

    assert result is None


def test_service_placement_chooses_between_multiple_nodes():
    placement = ServicePlacement()

    weak_node = ResourceNodeCandidate(
        node_id="node-weak",
        available=True,
        running_services=0,
        resources=make_resources(
            cpu_cores=2,
            cpu_usage_percent=30,
            memory_total_mb=4096,
            memory_used_mb=3000,
            disk_total_gb=50,
            disk_used_gb=40,
        ),
    )

    strong_node = ResourceNodeCandidate(
        node_id="node-strong",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=16,
            cpu_usage_percent=15,
            memory_total_mb=32768,
            memory_used_mb=4096,
            disk_total_gb=500,
            disk_used_gb=50,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=4,
        memory_mb=8192,
        disk_gb=50,
    )

    result = placement.select_node(
        service_id="service-heavy",
        requirements=requirements,
        nodes=[
            weak_node,
            strong_node,
        ],
    )

    assert result is not None
    assert result.node_id == "node-strong"


def test_service_placement_can_exclude_node():
    placement = ServicePlacement()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        available=True,
        running_services=1,
        resources=make_resources(),
    )

    requirements = ServiceRequirements(
        cpu_cores=1,
        memory_mb=1024,
        disk_gb=5,
    )

    result = placement.select_node(
        service_id="service-1",
        requirements=requirements,
        nodes=[
            node_a,
            node_b,
        ],
        exclude_node_id="node-a",
    )

    assert result is not None
    assert result.node_id == "node-b"


def test_service_placement_requires_service_id():
    placement = ServicePlacement()

    requirements = ServiceRequirements()

    with pytest.raises(ValueError):
        placement.select_node(
            service_id="",
            requirements=requirements,
            nodes=[],
        )


def test_service_placement_requires_valid_requirements():
    placement = ServicePlacement()

    with pytest.raises(TypeError):
        placement.select_node(
            service_id="service-1",
            requirements=None,
            nodes=[],
        )