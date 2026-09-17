import asyncio

import pytest

from freemesh.service import ServiceStatus
from freemesh.service_manager import ServiceManager


@pytest.mark.asyncio
async def test_service_manager_start_service():
    manager = ServiceManager()

    service = await manager.start_service(
        service_id="test-service",
        command="python -c \"import time; time.sleep(10)\"",
    )

    assert service.service_id == "test-service"
    assert service.status == ServiceStatus.RUNNING
    assert service.pid is not None
    assert manager.has_service("test-service")
    assert manager.get_service("test-service") is service
    assert manager.get_process("test-service") is not None

    await manager.stop_service("test-service")


@pytest.mark.asyncio
async def test_service_manager_stop_service():
    manager = ServiceManager()

    service = await manager.start_service(
        service_id="stop-service",
        command="python -c \"import time; time.sleep(10)\"",
    )

    assert service.status == ServiceStatus.RUNNING

    stopped_service = await manager.stop_service(
        "stop-service"
    )

    assert stopped_service.status == ServiceStatus.STOPPED
    assert manager.has_service("stop-service") is False
    assert manager.get_service("stop-service") is None
    assert manager.get_process("stop-service") is None


@pytest.mark.asyncio
async def test_service_manager_list_services():
    manager = ServiceManager()

    await manager.start_service(
        service_id="service-one",
        command="python -c \"import time; time.sleep(10)\"",
    )

    await manager.start_service(
        service_id="service-two",
        command="python -c \"import time; time.sleep(10)\"",
    )

    services = manager.list_services()

    assert len(services) == 2
    assert {service.service_id for service in services} == {
        "service-one",
        "service-two",
    }

    await manager.stop_all()

    assert manager.list_services() == []


@pytest.mark.asyncio
async def test_service_manager_rejects_duplicate_running_service():
    manager = ServiceManager()

    await manager.start_service(
        service_id="duplicate-service",
        command="python -c \"import time; time.sleep(10)\"",
    )

    with pytest.raises(RuntimeError):
        await manager.start_service(
            service_id="duplicate-service",
            command="python -c \"import time; time.sleep(10)\"",
        )

    await manager.stop_service("duplicate-service")


@pytest.mark.asyncio
async def test_service_manager_requires_valid_arguments():
    manager = ServiceManager()

    with pytest.raises(ValueError):
        await manager.start_service(
            service_id="",
            command="python -c \"print('test')\"",
        )

    with pytest.raises(ValueError):
        await manager.start_service(
            service_id="test-service",
            command="",
        )


@pytest.mark.asyncio
async def test_service_manager_stop_missing_service():
    manager = ServiceManager()

    with pytest.raises(KeyError):
        await manager.stop_service("missing-service")