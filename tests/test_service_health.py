"""Tests for NodeForge service health state."""

import pytest

from freemesh.service import (
    Service,
    ServiceHealth,
    ServiceStatus,
)


def test_new_service_health_is_unknown():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    assert service.status == ServiceStatus.STOPPED
    assert service.health == ServiceHealth.UNKNOWN
    assert service.is_healthy() is False


def test_running_service_can_be_marked_healthy():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    service.mark_healthy()

    assert service.status == ServiceStatus.RUNNING
    assert service.health == ServiceHealth.HEALTHY
    assert service.is_healthy() is True


def test_running_service_can_be_marked_unhealthy():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    service.mark_unhealthy()

    assert service.status == ServiceStatus.RUNNING
    assert service.health == ServiceHealth.UNHEALTHY
    assert service.is_healthy() is False


def test_crashed_service_is_unhealthy():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    service.mark_crashed()

    assert service.status == ServiceStatus.CRASHED
    assert service.health == ServiceHealth.CRASHED
    assert service.is_healthy() is False


def test_stopping_service_becomes_unknown_health():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    service.mark_healthy()
    service.mark_stopping()

    assert service.status == ServiceStatus.STOPPING
    assert service.health == ServiceHealth.UNKNOWN
    assert service.is_healthy() is False


def test_stopped_service_has_unknown_health():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    service.mark_healthy()
    service.mark_stopped()

    assert service.status == ServiceStatus.STOPPED
    assert service.health == ServiceHealth.UNKNOWN
    assert service.pid is None
    assert service.node_id is None
    assert service.is_healthy() is False


def test_failed_service_is_unhealthy():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    service.mark_failed()

    assert service.status == ServiceStatus.FAILED
    assert service.health == ServiceHealth.UNHEALTHY
    assert service.is_healthy() is False


def test_only_running_service_can_be_marked_healthy():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    with pytest.raises(RuntimeError):
        service.mark_healthy()


def test_only_running_service_can_be_marked_unhealthy():
    service = Service(
        service_id="health-test",
        command="python3 -c 'import time; time.sleep(10)'",
    )

    with pytest.raises(RuntimeError):
        service.mark_unhealthy()