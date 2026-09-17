"""Tests for the NodeForge Restart Engine."""

import asyncio

import pytest

from freemesh.restart_engine import RestartEngine
from freemesh.service import (
    ServiceHealth,
    ServiceStatus,
)
from freemesh.service_manager import ServiceManager


@pytest.mark.asyncio
async def test_restart_engine_recovers_crashed_service():
    manager = ServiceManager(
        max_restart_attempts=3
    )

    service = await manager.start_service(
        service_id="restart-service",
        command=(
            "python3 -c "
            "\"import time; time.sleep(10)\""
        ),
    )

    process = manager.get_process(
        service.service_id
    )

    assert process is not None

    process.terminate()
    await process.wait()

    service.mark_crashed()

    engine = RestartEngine(
        max_restart_attempts=3
    )

    result = await engine.restart(
        service=service,
        service_manager=manager,
    )

    try:
        assert result is True

        assert (
            service.status
            == ServiceStatus.RUNNING
        )

        assert (
            service.health
            == ServiceHealth.HEALTHY
        )

        assert service.restart_attempts == 1

        new_process = manager.get_process(
            service.service_id
        )

        assert new_process is not None
        assert new_process.pid != process.pid

    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_engine_counts_failed_attempts():
    manager = ServiceManager(
        max_restart_attempts=3
    )

    service = await manager.start_service(
        service_id="failed-restart-service",
        command=(
            "python3 -c "
            "\"raise SystemExit(1)\""
        ),
    )

    process = manager.get_process(
        service.service_id
    )

    assert process is not None

    await process.wait()

    service.mark_crashed()

    engine = RestartEngine(
        max_restart_attempts=3
    )

    result = await engine.restart(
        service=service,
        service_manager=manager,
    )

    try:
        assert result is False

        assert (
            service.restart_attempts == 3
        )

        assert (
            service.status
            == ServiceStatus.CRASHED
        )

        assert (
            service.health
            == ServiceHealth.CRASHED
        )

    finally:
        manager._services.pop(
            service.service_id,
            None,
        )

        manager._service_models.pop(
            service.service_id,
            None,
        )


@pytest.mark.asyncio
async def test_restart_engine_respects_max_attempts():
    manager = ServiceManager(
        max_restart_attempts=3
    )

    service = await manager.start_service(
        service_id="max-attempts-service",
        command=(
            "python3 -c "
            "\"import time; time.sleep(10)\""
        ),
    )

    process = manager.get_process(
        service.service_id
    )

    assert process is not None

    process.terminate()
    await process.wait()

    service.mark_crashed()

    service.restart_attempts = 3

    engine = RestartEngine(
        max_restart_attempts=3
    )

    result = await engine.restart(
        service=service,
        service_manager=manager,
    )

    try:
        assert result is False

        assert (
            service.restart_attempts == 3
        )

    finally:
        manager._services.pop(
            service.service_id,
            None,
        )

        manager._service_models.pop(
            service.service_id,
            None,
        )


@pytest.mark.asyncio
async def test_restart_engine_applies_backoff():
    manager = ServiceManager(
        max_restart_attempts=2
    )

    service = await manager.start_service(
        service_id="backoff-service",
        command=(
            "python3 -c "
            "\"raise SystemExit(1)\""
        ),
    )

    process = manager.get_process(
        service.service_id
    )

    assert process is not None

    await process.wait()

    service.mark_crashed()

    engine = RestartEngine(
        max_restart_attempts=2,
        backoff_seconds=0.05,
    )

    start_time = asyncio.get_running_loop().time()

    result = await engine.restart(
        service=service,
        service_manager=manager,
    )

    elapsed = (
        asyncio.get_running_loop().time()
        - start_time
    )

    try:
        assert result is False

        assert (
            service.restart_attempts == 2
        )

        assert elapsed >= 0.05

    finally:
        manager._services.pop(
            service.service_id,
            None,
        )

        manager._service_models.pop(
            service.service_id,
            None,
        )