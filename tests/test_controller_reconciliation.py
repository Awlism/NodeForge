import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState


@pytest.mark.asyncio
async def test_controller_reconciles_missing_running_service():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-1",
        command="python -c 'import time; time.sleep(10)'",
        desired_state=DesiredState.RUNNING,
    )

    calls = []

    async def fake_start_service(
        service_id,
        command,
        requirements=None,
    ):
        calls.append(
            (
                service_id,
                command,
                requirements,
            )
        )

    controller.start_service = fake_start_service

    result = await controller.reconcile_service(
        "service-1"
    )

    assert result.service_id == "service-1"
    assert result.action == "start"
    assert result.changed is True
    assert result.reason == "service_missing"

    assert len(calls) == 1
    assert calls[0][0] == "service-1"


@pytest.mark.asyncio
async def test_controller_reconciles_running_service_without_action():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-2",
        command="python app.py",
        desired_state=DesiredState.RUNNING,
    )

    service = controller.service_registry.register_service(
        service_id="service-2",
        node_id="node-a",
        status="running",
        pid=12345,
        command="python app.py",
    )

    calls = []

    async def fake_start_service(*args, **kwargs):
        calls.append("start")

    async def fake_stop_service(*args, **kwargs):
        calls.append("stop")

    async def fake_migrate_service(*args, **kwargs):
        calls.append("migrate")

    controller.start_service = fake_start_service
    controller.stop_service = fake_stop_service
    controller.migrate_service = fake_migrate_service

    result = await controller.reconcile_service(
        "service-2"
    )

    assert result.action == "none"
    assert result.changed is False
    assert result.reason == "already_running"
    assert calls == []


@pytest.mark.asyncio
async def test_controller_reconciles_stopped_intent():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-3",
        command="python app.py",
        desired_state=DesiredState.STOPPED,
    )

    controller.service_registry.register_service(
        service_id="service-3",
        node_id="node-a",
        status="running",
        pid=12345,
        command="python app.py",
    )

    calls = []

    async def fake_stop_service(service_id):
        calls.append(service_id)

    controller.stop_service = fake_stop_service

    result = await controller.reconcile_service(
        "service-3"
    )

    assert result.action == "stop"
    assert result.changed is True
    assert result.reason == "desired_state_stopped"
    assert calls == ["service-3"]


@pytest.mark.asyncio
async def test_controller_reconcile_all_services():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-4",
        command="python app.py",
        desired_state=DesiredState.RUNNING,
    )

    controller.create_service_intent(
        service_id="service-5",
        command="python worker.py",
        desired_state=DesiredState.STOPPED,
    )

    controller.service_registry.register_service(
        service_id="service-5",
        node_id="node-a",
        status="running",
        pid=54321,
        command="python worker.py",
    )

    start_calls = []
    stop_calls = []

    async def fake_start_service(
        service_id,
        command,
        requirements=None,
    ):
        start_calls.append(service_id)

    async def fake_stop_service(service_id):
        stop_calls.append(service_id)

    controller.start_service = fake_start_service
    controller.stop_service = fake_stop_service

    results = await controller.reconcile_all_services()

    assert len(results) == 2

    result_by_id = {
        result.service_id: result
        for result in results
    }

    assert result_by_id["service-4"].action == "start"
    assert result_by_id["service-4"].changed is True

    assert result_by_id["service-5"].action == "stop"
    assert result_by_id["service-5"].changed is True

    assert start_calls == ["service-4"]
    assert stop_calls == ["service-5"]