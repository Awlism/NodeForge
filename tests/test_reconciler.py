import pytest

from freemesh.controller.reconciler import (
    Reconciler,
)
from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.controller.service_intent_registry import (
    ServiceIntentRegistry,
)
from freemesh.service import (
    Service,
    ServiceStatus,
)


def create_reconciler():
    registry = ServiceIntentRegistry()

    return (
        Reconciler(registry),
        registry,
    )


@pytest.mark.asyncio
async def test_reconciler_starts_missing_running_service():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-1",
            desired_state=DesiredState.RUNNING,
            command="python app.py",
        )
    )

    calls = []

    async def start_service(
        service_id,
        command,
        requirements,
    ):
        calls.append(
            (
                service_id,
                command,
                requirements,
            )
        )

    async def stop_service(service_id):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-1",
        actual_service=None,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "start"
    assert result.changed is True
    assert result.reason == "service_missing"

    assert len(calls) == 1
    assert calls[0][0] == "service-1"
    assert calls[0][1] == "python app.py"


@pytest.mark.asyncio
async def test_reconciler_does_nothing_for_running_service():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-2",
            desired_state=DesiredState.RUNNING,
            command="python app.py",
        )
    )

    service = Service(
        service_id="service-2",
        command="python app.py",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    async def start_service(*args, **kwargs):
        raise AssertionError(
            "start_service should not be called"
        )

    async def stop_service(*args, **kwargs):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-2",
        actual_service=service,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "none"
    assert result.changed is False
    assert result.reason == "already_running"


@pytest.mark.asyncio
async def test_reconciler_restarts_stopped_service():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-3",
            desired_state=DesiredState.RUNNING,
            command="python app.py",
        )
    )

    service = Service(
        service_id="service-3",
        command="python app.py",
    )

    service.mark_stopped()

    calls = []

    async def start_service(
        service_id,
        command,
        requirements,
    ):
        calls.append(service_id)

    async def stop_service(*args, **kwargs):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-3",
        actual_service=service,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "start"
    assert result.changed is True
    assert result.reason == "actual_state_stopped"
    assert calls == ["service-3"]


@pytest.mark.asyncio
async def test_reconciler_restarts_failed_service():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-4",
            desired_state=DesiredState.RUNNING,
            command="python app.py",
        )
    )

    service = Service(
        service_id="service-4",
        command="python app.py",
    )

    service.mark_failed()

    calls = []

    async def start_service(
        service_id,
        command,
        requirements,
    ):
        calls.append(service_id)

    async def stop_service(*args, **kwargs):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-4",
        actual_service=service,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "start"
    assert result.changed is True
    assert result.reason == "actual_state_failed"
    assert calls == ["service-4"]


@pytest.mark.asyncio
async def test_reconciler_stops_service_when_desired_state_is_stopped():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-5",
            desired_state=DesiredState.STOPPED,
            command="python app.py",
        )
    )

    service = Service(
        service_id="service-5",
        command="python app.py",
    )

    service.mark_running(
        pid=12345,
        node_id="node-a",
    )

    calls = []

    async def start_service(*args, **kwargs):
        raise AssertionError(
            "start_service should not be called"
        )

    async def stop_service(service_id):
        calls.append(service_id)

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-5",
        actual_service=service,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "stop"
    assert result.changed is True
    assert result.reason == "desired_state_stopped"
    assert calls == ["service-5"]


@pytest.mark.asyncio
async def test_reconciler_does_nothing_when_stopped_service_is_absent():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-6",
            desired_state=DesiredState.STOPPED,
            command="python app.py",
        )
    )

    async def start_service(*args, **kwargs):
        raise AssertionError(
            "start_service should not be called"
        )

    async def stop_service(*args, **kwargs):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-6",
        actual_service=None,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "none"
    assert result.changed is False
    assert result.reason == "already_absent"


@pytest.mark.asyncio
async def test_reconciler_reports_missing_intent():
    reconciler, _ = create_reconciler()

    async def start_service(*args, **kwargs):
        raise AssertionError(
            "start_service should not be called"
        )

    async def stop_service(*args, **kwargs):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="unknown",
        actual_service=None,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "none"
    assert result.changed is False
    assert result.reason == "no_intent"


@pytest.mark.asyncio
async def test_reconciler_blocks_running_intent_without_command():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-7",
            desired_state=DesiredState.RUNNING,
            command=None,
        )
    )

    async def start_service(*args, **kwargs):
        raise AssertionError(
            "start_service should not be called"
        )

    async def stop_service(*args, **kwargs):
        raise AssertionError(
            "stop_service should not be called"
        )

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    result = await reconciler.reconcile(
        service_id="service-7",
        actual_service=None,
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert result.action == "blocked"
    assert result.changed is False
    assert result.reason == "missing_command"


@pytest.mark.asyncio
async def test_reconciler_handles_multiple_services():
    reconciler, registry = create_reconciler()

    registry.register(
        ServiceIntent(
            service_id="service-8",
            desired_state=DesiredState.RUNNING,
            command="python app.py",
        )
    )

    registry.register(
        ServiceIntent(
            service_id="service-9",
            desired_state=DesiredState.STOPPED,
            command="python worker.py",
        )
    )

    running_service = Service(
        service_id="service-9",
        command="python worker.py",
    )

    running_service.mark_running(
        pid=54321,
        node_id="node-b",
    )

    start_calls = []
    stop_calls = []

    async def start_service(
        service_id,
        command,
        requirements,
    ):
        start_calls.append(service_id)

    async def stop_service(service_id):
        stop_calls.append(service_id)

    async def migrate_service(*args, **kwargs):
        raise AssertionError(
            "migrate_service should not be called"
        )

    results = await reconciler.reconcile_all(
        actual_services={
            "service-9": running_service,
        },
        start_service=start_service,
        stop_service=stop_service,
        migrate_service=migrate_service,
    )

    assert len(results) == 2

    assert results[0].service_id == "service-8"
    assert results[0].action == "start"

    assert results[1].service_id == "service-9"
    assert results[1].action == "stop"

    assert start_calls == ["service-8"]
    assert stop_calls == ["service-9"]