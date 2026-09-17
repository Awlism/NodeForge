from freemesh.service import Service, ServiceStatus


def test_service_creation():
    service = Service(
        service_id="test-service",
        command="python -c \"print('hello')\"",
    )

    assert service.service_id == "test-service"
    assert service.command == "python -c \"print('hello')\""
    assert service.status == ServiceStatus.STOPPED
    assert service.pid is None
    assert service.restart_attempts == 0
    assert service.max_restart_attempts == 3


def test_service_lifecycle_transitions():
    service = Service(
        service_id="lifecycle-service",
        command="python -c \"print('hello')\"",
    )

    assert service.status == ServiceStatus.STOPPED

    service.status = ServiceStatus.STARTING
    assert service.status == ServiceStatus.STARTING

    service.status = ServiceStatus.RUNNING
    service.pid = 12345

    assert service.status == ServiceStatus.RUNNING
    assert service.pid == 12345

    service.status = ServiceStatus.STOPPING
    assert service.status == ServiceStatus.STOPPING

    service.status = ServiceStatus.STOPPED
    service.pid = None

    assert service.status == ServiceStatus.STOPPED
    assert service.pid is None