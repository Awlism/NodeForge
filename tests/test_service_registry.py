from freemesh.controller.service_registry import ServiceRegistry


def test_service_registry_register_and_get():
    registry = ServiceRegistry()

    service = registry.register_service(
        service_id="service-1",
        node_id="node-1",
        status="running",
        pid=1234,
        command="python bot.py",
    )

    assert service.service_id == "service-1"
    assert service.node_id == "node-1"
    assert service.status == "running"
    assert service.pid == 1234
    assert service.command == "python bot.py"

    stored = registry.get_service("service-1")

    assert stored is service


def test_service_registry_update():
    registry = ServiceRegistry()

    registry.register_service(
        service_id="service-1",
        node_id="node-1",
        status="starting",
        pid=100,
    )

    service = registry.update_service(
        service_id="service-1",
        status="running",
        pid=200,
    )

    assert service.status == "running"
    assert service.pid == 200


def test_service_registry_list_node_services():
    registry = ServiceRegistry()

    registry.register_service(
        service_id="service-1",
        node_id="node-1",
        status="running",
    )

    registry.register_service(
        service_id="service-2",
        node_id="node-1",
        status="stopped",
    )

    registry.register_service(
        service_id="service-3",
        node_id="node-2",
        status="running",
    )

    services = registry.list_node_services("node-1")

    assert len(services) == 2

    service_ids = {
        service.service_id
        for service in services
    }

    assert service_ids == {
        "service-1",
        "service-2",
    }


def test_service_registry_remove():
    registry = ServiceRegistry()

    registry.register_service(
        service_id="service-1",
        node_id="node-1",
        status="running",
    )

    removed = registry.remove_service("service-1")

    assert removed is not None
    assert registry.get_service("service-1") is None
    assert registry.service_count() == 0