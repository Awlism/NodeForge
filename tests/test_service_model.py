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

    service.mark_starting()
    assert service.status == ServiceStatus.STARTING

    service.mark_running(pid=12345)
    assert service.status == ServiceStatus.RUNNING
    assert service.pid == 12345

    service.mark_stopping()
    assert service.status == ServiceStatus.STOPPING

    service.mark_stopped()
    assert service.status == ServiceStatus.STOPPED
    assert service.pid is None


def test_service_crash_transition():
    service = Service(
        service_id="crash-service",
        command="python -c \"print('hello')\"",
    )

    service.mark_starting()
    service.mark_running(pid=12345)
    service.mark_crashed()

    assert service.status == ServiceStatus.CRASHED
    assert service.pid == 12345


def test_service_failed_transition():
    service = Service(
        service_id="failed-service",
        command="invalid-command",
    )

    service.mark_starting()
    service.mark_failed()

    assert service.status == ServiceStatus.FAILED
    assert service.pid is None