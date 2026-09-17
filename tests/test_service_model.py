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