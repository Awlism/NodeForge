"""Tests for NodeAgent service health monitoring."""

import asyncio

import pytest

from freemesh.node.agent import NodeAgent
from freemesh.service import (
    ServiceHealth,
    ServiceStatus,
)


@pytest.mark.asyncio
async def test_node_agent_detects_crashed_service():
    agent = NodeAgent(
        node_id="health-node",
        controller_host="127.0.0.1",
        controller_port=9999,
    )

    service = await agent._service_manager.start_service(
        service_id="health-service",
        command=(
            "python3 -c "
            "\"import time; time.sleep(0.2)\""
        ),
    )

    process = (
        agent._service_manager.get_process(
            service.service_id
        )
    )

    assert process is not None

    agent._running = True

    monitor_task = asyncio.create_task(
        agent._service_monitor_loop()
    )

    try:
        await asyncio.sleep(1.0)

        assert (
            service.status
            == ServiceStatus.RUNNING
        )

        assert (
            service.health
            == ServiceHealth.HEALTHY
        )

    finally:
        agent._running = False

        monitor_task.cancel()

        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        current_process = (
            agent._service_manager.get_process(
                service.service_id
            )
        )

        if (
            current_process is not None
            and current_process.returncode is None
        ):
            current_process.terminate()

            try:
                await asyncio.wait_for(
                    current_process.wait(),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                current_process.kill()
                await current_process.wait()

        agent._service_manager._services.pop(
            service.service_id,
            None,
        )

        agent._service_manager._service_models.pop(
            service.service_id,
            None,
        )


@pytest.mark.asyncio
async def test_node_agent_marks_live_service_healthy():
    agent = NodeAgent(
        node_id="healthy-node",
        controller_host="127.0.0.1",
        controller_port=9999,
    )

    service = await agent._service_manager.start_service(
        service_id="healthy-service",
        command=(
            "python3 -c "
            "\"import time; time.sleep(10)\""
        ),
    )

    agent._running = True

    monitor_task = asyncio.create_task(
        agent._service_monitor_loop()
    )

    try:
        await asyncio.sleep(0.8)

        assert (
            service.status
            == ServiceStatus.RUNNING
        )

        assert (
            service.health
            == ServiceHealth.HEALTHY
        )

    finally:
        agent._running = False

        monitor_task.cancel()

        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        await agent._service_manager.stop_all()


@pytest.mark.asyncio
async def test_health_checker_detects_process_exit():
    agent = NodeAgent(
        node_id="crash-detection-node",
        controller_host="127.0.0.1",
        controller_port=9999,
    )

    process = await asyncio.create_subprocess_shell(
        "python3 -c \"import time; time.sleep(0.2)\""
    )

    service = await agent._service_manager.start_service(
        service_id="crash-detection-service",
        command=(
            "python3 -c "
            "\"import time; time.sleep(0.2)\""
        ),
    )

    old_process = (
        agent._service_manager._services[
            service.service_id
        ]
    )

    if old_process.returncode is None:
        old_process.terminate()
        await old_process.wait()

    agent._service_manager._services[
        service.service_id
    ] = process

    service.pid = process.pid
    service.status = ServiceStatus.RUNNING

    await process.wait()

    health = agent._health_checker.check(
        service
    )

    assert health == ServiceHealth.CRASHED

    assert (
        service.status
        == ServiceStatus.CRASHED
    )

    assert (
        service.health
        == ServiceHealth.CRASHED
    )

    agent._service_manager._services.pop(
        service.service_id,
        None,
    )

    agent._service_manager._service_models.pop(
        service.service_id,
        None,
    )