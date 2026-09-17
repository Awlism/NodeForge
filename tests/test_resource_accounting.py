from freemesh.controller.resource_accounting import (
    ResourceAccounting,
)


def test_reserve_and_get_resource_reservation():
    accounting = ResourceAccounting()

    reservation = accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=2.0,
        memory_mb=2048,
        disk_gb=5,
    )

    assert reservation.service_id == "service-1"
    assert reservation.node_id == "node-a"

    stored = accounting.get("service-1")

    assert stored == reservation


def test_release_resource_reservation():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=1,
        memory_mb=512,
        disk_gb=2,
    )

    released = accounting.release(
        "service-1"
    )

    assert released is not None
    assert accounting.get("service-1") is None


def test_move_resource_reservation():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=5,
    )

    moved = accounting.move(
        service_id="service-1",
        target_node_id="node-b",
    )

    assert moved.node_id == "node-b"
    assert moved.cpu_cores == 2
    assert moved.memory_mb == 2048
    assert moved.disk_gb == 5


def test_node_usage():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=1,
        memory_mb=512,
        disk_gb=2,
    )

    accounting.reserve(
        service_id="service-2",
        node_id="node-a",
        cpu_cores=2,
        memory_mb=1024,
        disk_gb=3,
    )

    usage = accounting.node_usage(
        "node-a"
    )

    assert usage["cpu_cores"] == 3
    assert usage["memory_mb"] == 1536
    assert usage["disk_gb"] == 5


def test_node_usage_after_migration():
    accounting = ResourceAccounting()

    accounting.reserve(
        service_id="service-1",
        node_id="node-a",
        cpu_cores=2,
        memory_mb=2048,
        disk_gb=5,
    )

    accounting.move(
        service_id="service-1",
        target_node_id="node-b",
    )

    assert accounting.node_usage(
        "node-a"
    ) == {
        "cpu_cores": 0,
        "memory_mb": 0,
        "disk_gb": 0,
    }

    assert accounting.node_usage(
        "node-b"
    ) == {
        "cpu_cores": 2,
        "memory_mb": 2048,
        "disk_gb": 5,
    }


def test_service_count_per_node():
    accounting = ResourceAccounting()

    accounting.reserve(
        "service-1",
        "node-a",
        1,
        512,
        1,
    )

    accounting.reserve(
        "service-2",
        "node-a",
        1,
        512,
        1,
    )

    accounting.reserve(
        "service-3",
        "node-b",
        1,
        512,
        1,
    )

    assert accounting.service_count("node-a") == 2
    assert accounting.service_count("node-b") == 1
    assert accounting.service_count() == 3


def test_negative_resources_are_rejected():
    accounting = ResourceAccounting()

    try:
        accounting.reserve(
            "service-1",
            "node-a",
            cpu_cores=-1,
        )
        assert False
    except ValueError:
        assert True