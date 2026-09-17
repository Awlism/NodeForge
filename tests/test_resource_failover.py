"""Tests for resource-aware service failover and migration."""

from freemesh.controller.resource_failover import (
    ResourceFailover,
)
from freemesh.node.resources import NodeResources
from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
)
from freemesh.service_requirements import ServiceRequirements


def make_node(
    node_id: str,
    cpu_cores: float,
    cpu_usage_percent: float,
    memory_total_mb: int,
    memory_used_mb: int,
    disk_total_gb: float,
    disk_used_gb: float,
    running_services: int = 0,
) -> ResourceNodeCandidate:
    return ResourceNodeCandidate(
        node_id=node_id,
        available=True,
        running_services=running_services,
        resources=NodeResources(
            cpu_cores=cpu_cores,
            cpu_usage_percent=cpu_usage_percent,
            memory_total_mb=memory_total_mb,
            memory_used_mb=memory_used_mb,
            disk_total_gb=disk_total_gb,
            disk_used_gb=disk_used_gb,
            running_services=running_services,
        ),
    )


def test_select_replacement_node_with_enough_resources():
    failover = ResourceFailover()

    nodes = [
        make_node(
            node_id="failed-node",
            cpu_cores=8,
            cpu_usage_percent=10,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
        make_node(
            node_id="small-node",
            cpu_cores=2,
            cpu_usage_percent=20,
            memory_total_mb=2000,
            memory_used_mb=1000,
            disk_total_gb=20,
            disk_used_gb=10,
        ),
        make_node(
            node_id="large-node",
            cpu_cores=8,
            cpu_usage_percent=15,
            memory_total_mb=16000,
            memory_used_mb=3000,
            disk_total_gb=100,
            disk_used_gb=25,
        ),
    ]

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=5,
    )

    replacement = failover.select_replacement_node(
        nodes=nodes,
        failed_node_id="failed-node",
        requirements=requirements,
    )

    assert replacement is not None
    assert replacement.node_id == "large-node"


def test_failed_node_is_never_selected():
    failover = ResourceFailover()

    nodes = [
        make_node(
            node_id="failed-node",
            cpu_cores=16,
            cpu_usage_percent=5,
            memory_total_mb=32000,
            memory_used_mb=1000,
            disk_total_gb=500,
            disk_used_gb=20,
        ),
        make_node(
            node_id="healthy-node",
            cpu_cores=4,
            cpu_usage_percent=20,
            memory_total_mb=8000,
            memory_used_mb=1000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    ]

    requirements = ServiceRequirements(
        cpu_cores=1,
        memory_mb=512,
        disk_gb=2,
    )

    replacement = failover.select_replacement_node(
        nodes=nodes,
        failed_node_id="failed-node",
        requirements=requirements,
    )

    assert replacement is not None
    assert replacement.node_id == "healthy-node"


def test_no_replacement_when_capacity_is_insufficient():
    failover = ResourceFailover()

    nodes = [
        make_node(
            node_id="failed-node",
            cpu_cores=8,
            cpu_usage_percent=10,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
        make_node(
            node_id="small-node",
            cpu_cores=2,
            cpu_usage_percent=80,
            memory_total_mb=2000,
            memory_used_mb=1800,
            disk_total_gb=20,
            disk_used_gb=19,
        ),
    ]

    requirements = ServiceRequirements(
        cpu_cores=4,
        memory_mb=4096,
        disk_gb=20,
    )

    replacement = failover.select_replacement_node(
        nodes=nodes,
        failed_node_id="failed-node",
        requirements=requirements,
    )

    assert replacement is None


def test_create_migration_plan():
    failover = ResourceFailover()

    nodes = [
        make_node(
            node_id="node-a",
            cpu_cores=8,
            cpu_usage_percent=80,
            memory_total_mb=16000,
            memory_used_mb=12000,
            disk_total_gb=100,
            disk_used_gb=50,
        ),
        make_node(
            node_id="node-b",
            cpu_cores=8,
            cpu_usage_percent=15,
            memory_total_mb=16000,
            memory_used_mb=3000,
            disk_total_gb=100,
            disk_used_gb=25,
        ),
    ]

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=5,
    )

    plan = failover.create_migration_plan(
        service_id="service-1",
        source_node_id="node-a",
        command="python3 bot.py",
        requirements=requirements,
        nodes=nodes,
    )

    assert plan is not None
    assert plan.service_id == "service-1"
    assert plan.source_node_id == "node-a"
    assert plan.target_node_id == "node-b"
    assert plan.command == "python3 bot.py"
    assert plan.requirements == requirements


def test_migration_plan_preserves_service_requirements():
    failover = ResourceFailover()

    requirements = ServiceRequirements(
        cpu_cores=3.5,
        memory_mb=4096,
        disk_gb=12,
    )

    nodes = [
        make_node(
            node_id="node-b",
            cpu_cores=8,
            cpu_usage_percent=10,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    ]

    plan = failover.create_migration_plan(
        service_id="service-2",
        source_node_id="node-a",
        command="python3 worker.py",
        requirements=requirements,
        nodes=nodes,
    )

    assert plan is not None
    assert plan.requirements.cpu_cores == 3.5
    assert plan.requirements.memory_mb == 4096
    assert plan.requirements.disk_gb == 12