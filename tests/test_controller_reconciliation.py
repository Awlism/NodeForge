import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import (
    DesiredState,
)
from freemesh.service import ServiceStatus
from freemesh.service_requirements import ServiceRequirements


@pytest.mark.asyncio
async def test_controller_reconciles_missing_running_service(
    monkeypatch,
):
    controller = Controller()

    controller.create_service_intent(
        service_id="service-1",
        desired_state=DesiredState.RUNNING,
        command="python -c 'import time; time.sleep(60)'",
    )

    calls = []

    async def fake_start_service_auto(
        service_id,
        command,
        *,
        required_cpu_cores=0.0,
        required_memory_mb=0,
        required_disk_gb=0.0,
    ):
        calls.append(
            {
                "service_id": service_id,
                "command": command,
                "required_cpu_cores": required_cpu_cores,
                "required_memory_mb": required_memory_mb,
                "required_disk_gb": required_disk_gb,
            }
        )

        return {
            "service_id": service_id,
            "status": "running",
        }

    monkeypatch.setattr(
        controller,
        "start_service_auto",
        fake_start_service_auto,
    )

    result = await controller.reconcile_service(
        "service-1"
    )

    assert result.action == "start"
    assert result.changed is True
    assert result.reason == "service_missing"

    assert len(calls) == 1
    assert calls[0]["service_id"] == "service-1"


@pytest.mark.asyncio
async def test_controller_reconciles_running_service_without_action():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-2",
        desired_state=DesiredState.RUNNING,
        command="python -c 'import time; time.sleep(60)'",
    )

    controller.service_registry.register_service(
        service_id="service-2",
        node_id="node-1",
        status=ServiceStatus.RUNNING,
        pid=12345,
        command="python -c 'import time; time.sleep(60)'",
        requirements=ServiceRequirements(),
    )

    result = await controller.reconcile_service(
        "service-2"
    )

    assert result.action == "none"
    assert result.changed is False
    assert result.reason == "already_running"


@pytest.mark.asyncio
async def test_controller_reconciles_stopped_intent(
    monkeypatch,
):
    controller = Controller()

    controller.create_service_intent(
        service_id="service-3",
        desired_state=DesiredState.STOPPED,
        command="python -c 'import time; time.sleep(60)'",
    )

    controller.service_registry.register_service(
        service_id="service-3",
        node_id="node-1",
        status=ServiceStatus.RUNNING,
        pid=12345,
        command="python -c 'import time; time.sleep(60)'",
        requirements=ServiceRequirements(),
    )

    calls = []

    async def fake_stop_service(
        node_id,
        service_id,
    ):
        calls.append(
            {
                "node_id": node_id,
                "service_id": service_id,
            }
        )

        return {
            "service_id": service_id,
            "status": "stopped",
        }

    monkeypatch.setattr(
        controller,
        "stop_service",
        fake_stop_service,
    )

    result = await controller.reconcile_service(
        "service-3"
    )

    assert result.action == "stop"
    assert result.changed is True
    assert result.reason == "desired_state_stopped"

    assert len(calls) == 1
    assert calls[0]["node_id"] == "node-1"
    assert calls[0]["service_id"] == "service-3"


@pytest.mark.asyncio
async def test_controller_reconcile_all_services(
    monkeypatch,
):
    controller = Controller()

    controller.create_service_intent(
        service_id="service-4",
        desired_state=DesiredState.RUNNING,
        command="python -c 'import time; time.sleep(60)'",
    )

    controller.create_service_intent(
        service_id="service-5",
        desired_state=DesiredState.RUNNING,
        command="python -c 'import time; time.sleep(60)'",
    )

    calls = []

    async def fake_start_service_auto(
        service_id,
        command,
        *,
        required_cpu_cores=0.0,
        required_memory_mb=0,
        required_disk_gb=0.0,
    ):
        calls.append(
            {
                "service_id": service_id,
                "command": command,
                "required_cpu_cores": required_cpu_cores,
                "required_memory_mb": required_memory_mb,
                "required_disk_gb": required_disk_gb,
            }
        )

        return {
            "service_id": service_id,
            "status": "running",
        }

    monkeypatch.setattr(
        controller,
        "start_service_auto",
        fake_start_service_auto,
    )

    results = await controller.reconcile_all_services()

    result_by_id = {
        result.service_id: result
        for result in results
    }

    assert result_by_id["service-4"].action == "start"
    assert result_by_id["service-5"].action == "start"

    assert result_by_id["service-4"].changed is True
    assert result_by_id["service-5"].changed is True

    assert len(calls) == 2

    assert {
        call["service_id"]
        for call in calls
    } == {
        "service-4",
        "service-5",
    }