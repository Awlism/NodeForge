from freemesh.controller.resource_accounting import (
    ResourceAccounting,
)
from freemesh.node.resources import NodeResources
from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
    ResourceScheduler,
)
from freemesh.service_requirements import ServiceRequirements


def make_resources(
    cpu_cores=4.0,
    cpu_usage_percent=20.0,
    memory_total_mb=8192,
    memory_used_mb=2048,
    disk_total_gb=100.0,
    disk_used_gb=20.0,
    running_services=0,
):
    return NodeResources(
        cpu_cores=cpu_cores,
        cpu_usage_percent=cpu_usage_percent,
        memory_total_mb=memory_total_mb,
        memory_used_mb=memory_used_mb,
        disk_total_gb=disk_total_gb,
        disk_used_gb=disk_used_gb,
        running_services=running_services,
    )


def test_scheduler_selects_available_node():
    scheduler = ResourceScheduler()

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=2,
        resources=make_resources(),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        available=True,
        running_services=1,
        resources=make_resources(),
    )

    selected = scheduler.select_node(
        [node_a, node_b]
    )

    assert selected is node_b


def test_scheduler_ignores_unavailable_nodes():
    scheduler = ResourceScheduler()

    unavailable = ResourceNodeCandidate(
        node_id="node-a",
        available=False,
        running_services=0,
        resources=make_resources(),
    )

    available = ResourceNodeCandidate(
        node_id="node-b",
        available=True,
        running_services=1,
        resources=make_resources(),
    )

    selected = scheduler.select_node(
        [unavailable, available]
    )

    assert selected is available


def test_scheduler_excludes_requested_node():
    scheduler = ResourceScheduler()

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

    selected = scheduler.select_node(
        [node_a, node_b],
        exclude_node_id="node-a",
    )

    assert selected is node_b


def test_scheduler_returns_none_without_candidates():
    scheduler = ResourceScheduler()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=False,
        running_services=0,
        resources=make_resources(),
    )

    selected = scheduler.select_node([node])

    assert selected is None


def test_scheduler_ignores_nodes_without_resource_information():
    scheduler = ResourceScheduler()

    node_without_resources = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=None,
    )

    node_with_resources = ResourceNodeCandidate(
        node_id="node-b",
        available=True,
        running_services=1,
        resources=make_resources(),
    )

    selected = scheduler.select_node(
        [node_without_resources, node_with_resources]
    )

    assert selected is node_with_resources


def test_scheduler_respects_cpu_capacity():
    scheduler = ResourceScheduler()

    low_cpu_node = ResourceNodeCandidate(
        node_id="node-low-cpu",
        available=True,
        running_services=0,
        resources=make_resources(
            cpu_cores=4,
            cpu_usage_percent=95,
        ),
    )

    healthy_node = ResourceNodeCandidate(
        node_id="node-healthy",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
        ),
    )

    selected = scheduler.select_node(
        [low_cpu_node, healthy_node],
        required_cpu_cores=2,
    )

    assert selected is healthy_node


def test_scheduler_respects_memory_capacity():
    scheduler = ResourceScheduler()

    low_memory_node = ResourceNodeCandidate(
        node_id="node-low-memory",
        available=True,
        running_services=0,
        resources=make_resources(
            memory_total_mb=2048,
            memory_used_mb=1900,
        ),
    )

    healthy_node = ResourceNodeCandidate(
        node_id="node-healthy",
        available=True,
        running_services=1,
        resources=make_resources(
            memory_total_mb=16384,
            memory_used_mb=2048,
        ),
    )

    selected = scheduler.select_node(
        [low_memory_node, healthy_node],
        required_memory_mb=4096,
    )

    assert selected is healthy_node


def test_scheduler_respects_disk_capacity():
    scheduler = ResourceScheduler()

    low_disk_node = ResourceNodeCandidate(
        node_id="node-low-disk",
        available=True,
        running_services=0,
        resources=make_resources(
            disk_total_gb=20,
            disk_used_gb=19,
        ),
    )

    healthy_node = ResourceNodeCandidate(
        node_id="node-healthy",
        available=True,
        running_services=1,
        resources=make_resources(
            disk_total_gb=200,
            disk_used_gb=20,
        ),
    )

    selected = scheduler.select_node(
        [low_disk_node, healthy_node],
        required_disk_gb=20,
    )

    assert selected is healthy_node


def test_scheduler_returns_none_when_capacity_is_insufficient():
    scheduler = ResourceScheduler()

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

    selected = scheduler.select_node(
        [node],
        required_cpu_cores=2,
        required_memory_mb=1024,
        required_disk_gb=5,
    )

    assert selected is None


def test_scheduler_accepts_service_requirements():
    scheduler = ResourceScheduler()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=4000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=10,
    )

    selected = scheduler.select_node_for_requirements(
        [node],
        requirements,
    )

    assert selected is node


def test_scheduler_rejects_requirements_without_capacity():
    scheduler = ResourceScheduler()

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
        memory_mb=1024,
        disk_gb=5,
    )

    selected = scheduler.select_node_for_requirements(
        [node],
        requirements,
    )

    assert selected is None


def test_scheduler_chooses_capacity_suitable_node():
    scheduler = ResourceScheduler()

    small_node = ResourceNodeCandidate(
        node_id="node-small",
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

    large_node = ResourceNodeCandidate(
        node_id="node-large",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=25,
            memory_total_mb=16000,
            memory_used_mb=4000,
            disk_total_gb=200,
            disk_used_gb=30,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=4096,
        disk_gb=20,
    )

    selected = scheduler.select_node_for_requirements(
        [small_node, large_node],
        requirements,
    )

    assert selected is large_node


def test_scheduler_rejects_negative_cpu_requirement():
    scheduler = ResourceScheduler()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(),
    )

    try:
        scheduler.select_node(
            [node],
            required_cpu_cores=-1,
        )
        assert False
    except ValueError:
        assert True


def test_scheduler_rejects_negative_memory_requirement():
    scheduler = ResourceScheduler()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(),
    )

    try:
        scheduler.select_node(
            [node],
            required_memory_mb=-1,
        )
        assert False
    except ValueError:
        assert True


def test_scheduler_rejects_negative_disk_requirement():
    scheduler = ResourceScheduler()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(),
    )

    try:
        scheduler.select_node(
            [node],
            required_disk_gb=-1,
        )
        assert False
    except ValueError:
        assert True


def test_scheduler_rejects_invalid_requirements_type():
    scheduler = ResourceScheduler()

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=0,
        resources=make_resources(),
    )

    try:
        scheduler.select_node_for_requirements(
            [node],
            None,
        )
        assert False
    except TypeError:
        assert True


# ============================================================
# D6.8 RESERVATION-AWARE SCHEDULING
# ============================================================


def test_scheduler_respects_resource_reservations():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="existing-service",
        node_id="node-a",
        cpu_cores=5.0,
        memory_mb=5000,
        disk_gb=50,
    )

    scheduler = ResourceScheduler(
        accounting=accounting,
    )

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    node_b = ResourceNodeCandidate(
        node_id="node-b",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=3,
        memory_mb=3000,
        disk_gb=20,
    )

    selected = scheduler.select_node_for_requirements(
        [node_a, node_b],
        requirements,
    )

    assert selected is node_b


def test_scheduler_allows_node_after_reservation_release():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="existing-service",
        node_id="node-a",
        cpu_cores=5.0,
        memory_mb=5000,
        disk_gb=50,
    )

    scheduler = ResourceScheduler(
        accounting=accounting,
    )

    node_a = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=3,
        memory_mb=3000,
        disk_gb=20,
    )

    blocked = scheduler.select_node_for_requirements(
        [node_a],
        requirements,
    )

    assert blocked is None

    accounting.release("existing-service")

    selected = scheduler.select_node_for_requirements(
        [node_a],
        requirements,
    )

    assert selected is node_a


def test_scheduler_reservation_does_not_double_count_live_usage():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=2.0,
        memory_mb=2048,
        disk_gb=10,
    )

    scheduler = ResourceScheduler(
        accounting=accounting,
    )

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=25,
            memory_total_mb=16000,
            memory_used_mb=2048,
            disk_total_gb=100,
            disk_used_gb=10,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=4,
        memory_mb=4096,
        disk_gb=20,
    )

    selected = scheduler.select_node_for_requirements(
        [node],
        requirements,
    )

    assert selected is node


def test_scheduler_blocks_node_when_reservations_exhaust_capacity():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=6.0,
        memory_mb=12000,
        disk_gb=70,
    )

    scheduler = ResourceScheduler(
        accounting=accounting,
    )

    node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=2,
        memory_mb=3000,
        disk_gb=20,
    )

    selected = scheduler.select_node_for_requirements(
        [node],
        requirements,
    )

    assert selected is None


def test_scheduler_reservation_aware_failover_selects_second_node():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="existing-service",
        node_id="node-a",
        cpu_cores=6.0,
        memory_mb=10000,
        disk_gb=70,
    )

    scheduler = ResourceScheduler(
        accounting=accounting,
    )

    failed_node = ResourceNodeCandidate(
        node_id="node-failed",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    reserved_node = ResourceNodeCandidate(
        node_id="node-a",
        available=True,
        running_services=1,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=20,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    healthy_node = ResourceNodeCandidate(
        node_id="node-b",
        available=True,
        running_services=0,
        resources=make_resources(
            cpu_cores=8,
            cpu_usage_percent=10,
            memory_total_mb=16000,
            memory_used_mb=2000,
            disk_total_gb=100,
            disk_used_gb=20,
        ),
    )

    requirements = ServiceRequirements(
        cpu_cores=3,
        memory_mb=4000,
        disk_gb=20,
    )

    selected = scheduler.select_node_for_requirements(
        [
            failed_node,
            reserved_node,
            healthy_node,
        ],
        requirements,
        exclude_node_id="node-failed",
    )

    assert selected is healthy_node