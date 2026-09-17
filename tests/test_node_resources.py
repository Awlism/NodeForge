from freemesh.node.resources import NodeResources


def test_resource_calculations():
    resources = NodeResources(
        cpu_cores=4,
        cpu_usage_percent=25.0,
        memory_total_mb=8192,
        memory_used_mb=2048,
        disk_total_gb=100.0,
        disk_used_gb=40.0,
        running_services=2,
    )

    assert resources.memory_available_mb == 6144
    assert resources.disk_available_gb == 60.0
    assert resources.memory_usage_percent == 25.0
    assert resources.disk_usage_percent == 40.0


def test_node_has_capacity():
    resources = NodeResources(
        cpu_cores=4,
        cpu_usage_percent=25.0,
        memory_total_mb=8192,
        memory_used_mb=2048,
        disk_total_gb=100.0,
        disk_used_gb=40.0,
    )

    assert resources.has_capacity(
        required_cpu_cores=1.0,
        required_memory_mb=1024,
        required_disk_gb=5.0,
    ) is True


def test_node_without_capacity():
    resources = NodeResources(
        cpu_cores=4,
        cpu_usage_percent=90.0,
        memory_total_mb=8192,
        memory_used_mb=7500,
        disk_total_gb=100.0,
        disk_used_gb=95.0,
    )

    assert resources.has_capacity(
        required_cpu_cores=1.0,
        required_memory_mb=1024,
        required_disk_gb=10.0,
    ) is False


def test_negative_requirements_are_rejected():
    resources = NodeResources(
        cpu_cores=4,
        cpu_usage_percent=20.0,
        memory_total_mb=8192,
        memory_used_mb=2048,
        disk_total_gb=100.0,
        disk_used_gb=40.0,
    )

    try:
        resources.has_capacity(
            required_cpu_cores=-1.0,
        )
        assert False
    except ValueError as exc:
        assert "required_cpu_cores" in str(exc)


def test_zero_capacity_resources_are_handled():
    resources = NodeResources(
        cpu_cores=0,
        cpu_usage_percent=0,
        memory_total_mb=0,
        memory_used_mb=0,
        disk_total_gb=0,
        disk_used_gb=0,
    )

    assert resources.memory_available_mb == 0
    assert resources.disk_available_gb == 0
    assert resources.memory_usage_percent == 100.0
    assert resources.disk_usage_percent == 100.0