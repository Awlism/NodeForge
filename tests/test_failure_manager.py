from freemesh.controller.failure_manager import FailureManager


def test_failure_manager_records_failure():
    manager = FailureManager()

    failure = manager.record_failure(
        service_id="service-1",
        node_id="node-1",
        status="crashed",
        reason="process exited unexpectedly",
        restart_attempts=3,
    )

    assert failure.service_id == "service-1"
    assert failure.node_id == "node-1"
    assert failure.status == "crashed"
    assert failure.restart_attempts == 3

    stored = manager.get_failure("service-1")

    assert stored is failure


def test_failure_manager_detects_failover_condition():
    manager = FailureManager()

    manager.record_failure(
        service_id="service-1",
        node_id="node-1",
        restart_attempts=3,
    )

    assert manager.should_failover(
        "service-1",
        max_restart_attempts=3,
    ) is True


def test_failure_manager_does_not_failover_before_limit():
    manager = FailureManager()

    manager.record_failure(
        service_id="service-1",
        node_id="node-1",
        restart_attempts=1,
    )

    assert manager.should_failover(
        "service-1",
        max_restart_attempts=3,
    ) is False