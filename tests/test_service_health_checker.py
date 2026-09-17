"""Tests for real NodeForge service health checks."""

import asyncio

import pytest

from freemesh.service import (
    Service,
    ServiceHealth,
    ServiceStatus,
)
from freemesh.service_health import ServiceHealthChecker


@pytest.mark.asyncio
async def test_live_process_is_healthy():
    process = await asyncio.create_subprocess_shell(
        "python3 -c "
        "\"import time; time.sleep(10)\""
    )

    try:
        service = Service(
            service_id="live-service",
            command=(
                "python3 -c "
                "\"import time; time.sleep(10)\""
            ),
        )

        service.mark_running(
            pid=process.pid,
            node_id="node-a",
        )

        checker = ServiceHealthChecker()

        health = checker.check(service)

        assert health == ServiceHealth.HEALTHY
        assert service.health == ServiceHealth.HEALTHY
        assert service.is_healthy() is True

    finally:
        process.terminate()
        await process.wait()


@pytest.mark.asyncio
async def test_dead_process_is_detected_as_crashed():
    process = await asyncio.create_subprocess_shell(
        "python3 -c \"pass\""
    )

    pid = process.pid

    await process.wait()

    service = Service(
        service_id="dead-service",
        command="python3 -c \"pass\"",
    )

    service.mark_running(
        pid=pid,
        node_id="node-a",
    )

    checker = ServiceHealthChecker()

    health = checker.check(service)

    assert health == ServiceHealth.CRASHED
    assert service.status == ServiceStatus.CRASHED
    assert service.health == ServiceHealth.CRASHED
    assert service.is_healthy() is False


def test_missing_pid_is_unhealthy():
    service = Service(
        service_id="missing-pid",
        command="python3 -c \"pass\"",
    )

    service.mark_running(
        pid=999999999,
        node_id="node-a",
    )

    checker = ServiceHealthChecker()

    health = checker.check(service)

    assert health == ServiceHealth.CRASHED
    assert service.status == ServiceStatus.CRASHED
    assert service.health == ServiceHealth.CRASHED


def test_none_pid_is_unhealthy():
    service = Service(
        service_id="no-pid",
        command="python3 -c \"pass\"",
    )

    service.status = ServiceStatus.RUNNING
    service.pid = None

    checker = ServiceHealthChecker()

    health = checker.check(service)

    assert health == ServiceHealth.UNHEALTHY
    assert service.health == ServiceHealth.UNHEALTHY


def test_stopped_service_has_unknown_health():
    service = Service(
        service_id="stopped-service",
        command="python3 -c \"pass\"",
    )

    checker = ServiceHealthChecker()

    health = checker.check(service)

    assert health == ServiceHealth.UNKNOWN
    assert service.health == ServiceHealth.UNKNOWN


def test_failed_service_is_unhealthy():
    service = Service(
        service_id="failed-service",
        command="python3 -c \"pass\"",
    )

    service.mark_failed()

    checker = ServiceHealthChecker()

    health = checker.check(service)

    assert health == ServiceHealth.UNHEALTHY
    assert service.health == ServiceHealth.UNHEALTHY


def test_process_exists_rejects_invalid_pid():
    checker = ServiceHealthChecker()

    assert checker.process_exists(0) is False
    assert checker.process_exists(-1) is False


def test_check_pid_rejects_none():
    checker = ServiceHealthChecker()

    assert checker.check_pid(None) is False