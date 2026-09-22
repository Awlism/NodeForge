"""Focused audit tests for NodeForge service orchestration layers."""

from types import SimpleNamespace

import pytest

from freemesh.controller.service_commands import ServiceCommandLayer
from freemesh.controller.service_health import ServiceHealthManager
from freemesh.controller.service_lifecycle import ServiceLifecycleManager
from freemesh.controller.service_recovery import ServiceRecoveryCoordinator
from freemesh.controller.service_orchestrator import ServiceOrchestrator


class FakeServiceRegistry:
    def __init__(self):
        self.service = SimpleNamespace(
            service_id="svc-1",
            node_id="node-a",
            status="running",
            pid=123,
        )

    def get_service(self, service_id):
        if service_id == self.service.service_id:
            return self.service
        return None

    def list_services(self):
        return [self.service]

    def list_node_services(self, node_id):
        if node_id == self.service.node_id:
            return [self.service]
        return []


class FakeController:
    def __init__(self):
        self.calls = []
        self.service_registry = FakeServiceRegistry()
        self._active_nodes = {"node-a": object()}

        self.reconciler = SimpleNamespace(
            reconcile=self._reconcile,
        )

    async def start_service_auto(self, **kwargs):
        self.calls.append(
            ("start_service_auto", kwargs)
        )
        return "started"

    async def stop_service(self, **kwargs):
        self.calls.append(
            ("stop_service", kwargs)
        )
        return "stopped"

    async def migrate_service(self, **kwargs):
        self.calls.append(
            ("migrate_service", kwargs)
        )
        return "migrated"

    async def status_service(self, **kwargs):
        self.calls.append(
            ("status_service", kwargs)
        )
        return SimpleNamespace(
            payload={
                "status": "running",
            }
        )

    async def _handle_service_failure(self, **kwargs):
        self.calls.append(
            ("handle_service_failure", kwargs)
        )
        return "recovered"

    async def _reconcile(self, **kwargs):
        self.calls.append(
            ("reconcile", kwargs)
        )
        return "reconciled"


@pytest.mark.asyncio
async def test_orchestrator_uses_current_controller_methods():
    """Orchestrator must not capture stale bound methods."""

    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    first = await orchestrator.ensure_running(
        service_id="svc-1",
        command="python worker.py",
    )

    assert first == "started"

    async def replacement_start(**kwargs):
        controller.calls.append(
            ("replacement_start", kwargs)
        )
        return "replacement"

    controller.start_service_auto = (
        replacement_start
    )

    second = await orchestrator.ensure_running(
        service_id="svc-1",
        command="python worker.py",
    )

    assert second == "replacement"
    assert (
        controller.calls[-1][0]
        == "replacement_start"
    )


@pytest.mark.asyncio
async def test_orchestrator_ensure_stopped():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(
        controller
    )

    result = await orchestrator.ensure_stopped(
        "svc-1"
    )

    assert result == "stopped"

    name, kwargs = controller.calls[-1]

    assert name == "stop_service"
    assert kwargs["node_id"] == "node-a"
    assert kwargs["service_id"] == "svc-1"


@pytest.mark.asyncio
async def test_orchestrator_ensure_stopped_missing_service():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(
        controller
    )

    result = await orchestrator.ensure_stopped(
        "missing"
    )

    assert result is None
    assert controller.calls == []


@pytest.mark.asyncio
async def test_orchestrator_recover():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(
        controller
    )

    result = await orchestrator.recover(
        "svc-1"
    )

    assert result == "migrated"

    name, kwargs = controller.calls[-1]

    assert name == "migrate_service"
    assert kwargs["service_id"] == "svc-1"
    assert kwargs["failed_node_id"] == "node-a"


@pytest.mark.asyncio
async def test_orchestrator_recover_missing_service():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(
        controller
    )

    result = await orchestrator.recover(
        "missing"
    )

    assert result is None
    assert controller.calls == []


@pytest.mark.asyncio
async def test_orchestrator_migrate():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(
        controller
    )

    result = await orchestrator.migrate(
        service_id="svc-1",
        failed_node_id="node-a",
        timeout_seconds=15.0,
    )

    assert result == "migrated"

    name, kwargs = controller.calls[-1]

    assert name == "migrate_service"
    assert kwargs["service_id"] == "svc-1"
    assert kwargs["failed_node_id"] == "node-a"
    assert kwargs["timeout_seconds"] == 15.0


@pytest.mark.asyncio
async def test_lifecycle_start_stop_restart():
    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    lifecycle = ServiceLifecycleManager(
        orchestrator
    )

    assert (
        await lifecycle.start(
            "svc-1",
            "python worker.py",
        )
        == "started"
    )

    assert (
        await lifecycle.stop("svc-1")
        == "stopped"
    )

    assert (
        await lifecycle.restart(
            "svc-1",
            "python worker.py",
        )
        == "started"
    )

    names = [
        call[0]
        for call in controller.calls
    ]

    assert names == [
        "start_service_auto",
        "stop_service",
        "stop_service",
        "start_service_auto",
    ]


@pytest.mark.asyncio
async def test_command_layer_delegation():
    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    lifecycle = ServiceLifecycleManager(
        orchestrator
    )

    recovery = ServiceRecoveryCoordinator(
        orchestrator
    )

    controller.service_lifecycle = lifecycle
    controller.service_orchestrator = orchestrator
    controller.service_recovery = recovery

    commands = ServiceCommandLayer(
        controller
    )

    assert (
        await commands.start(
            "svc-1",
            "python worker.py",
        )
        == "started"
    )

    assert (
        await commands.stop("svc-1")
        == "stopped"
    )

    assert (
        await commands.restart(
            "svc-1",
            "python worker.py",
        )
        == "started"
    )

    assert (
        await commands.recover("svc-1")
        == "migrated"
    )

    assert (
        await commands.migrate(
            "svc-1",
            "node-a",
        )
        == "migrated"
    )

    status = await commands.status(
        "svc-1"
    )

    assert status.payload["status"] == "running"


@pytest.mark.asyncio
async def test_command_layer_missing_status_service():
    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    lifecycle = ServiceLifecycleManager(
        orchestrator
    )

    recovery = ServiceRecoveryCoordinator(
        orchestrator
    )

    controller.service_lifecycle = lifecycle
    controller.service_orchestrator = orchestrator
    controller.service_recovery = recovery

    controller.service_registry.service = None

    commands = ServiceCommandLayer(
        controller
    )

    result = await commands.status(
        "missing"
    )

    assert result is None


@pytest.mark.asyncio
async def test_recovery_filters_wrong_node():
    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    recovery = ServiceRecoveryCoordinator(
        orchestrator
    )

    controller.service_registry.service.node_id = (
        "node-b"
    )

    result = await recovery.recover_failed_service(
        service_id="svc-1",
        node_id="node-a",
    )

    assert result is None
    assert controller.calls == []


@pytest.mark.asyncio
async def test_recovery_skips_stopped_service():
    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    recovery = ServiceRecoveryCoordinator(
        orchestrator
    )

    controller.service_registry.service.status = (
        "stopped"
    )

    await recovery.recover_from_node(
        "node-a"
    )

    assert not any(
        call[0] == "migrate_service"
        for call in controller.calls
    )


@pytest.mark.asyncio
async def test_recovery_skips_failed_service():
    controller = FakeController()

    orchestrator = ServiceOrchestrator(
        controller
    )

    recovery = ServiceRecoveryCoordinator(
        orchestrator
    )

    controller.service_registry.service.status = (
        "failed"
    )

    await recovery.recover_from_node(
        "node-a"
    )

    assert not any(
        call[0] == "migrate_service"
        for call in controller.calls
    )


@pytest.mark.asyncio
async def test_health_skips_stopped_service():
    controller = FakeController()

    controller.service_registry.service.status = (
        "stopped"
    )

    health = ServiceHealthManager(
        controller
    )

    result = await health.check_service(
        "svc-1"
    )

    assert result is None
    assert controller.calls == []


@pytest.mark.asyncio
async def test_health_skips_failed_service():
    controller = FakeController()

    controller.service_registry.service.status = (
        "failed"
    )

    health = ServiceHealthManager(
        controller
    )

    result = await health.check_service(
        "svc-1"
    )

    assert result is None
    assert controller.calls == []


@pytest.mark.asyncio
async def test_health_handles_missing_transport():
    controller = FakeController()

    controller._active_nodes.clear()

    health = ServiceHealthManager(
        controller
    )

    result = await health.check_service(
        "svc-1"
    )

    assert result == "recovered"

    assert (
        controller.calls[-1][0]
        == "handle_service_failure"
    )


@pytest.mark.asyncio
async def test_health_handles_runtime_failure():
    controller = FakeController()

    async def failed_status(**kwargs):
        return SimpleNamespace(
            payload={
                "status": "crashed",
                "restart_attempts": 2,
            }
        )

    controller.status_service = (
        failed_status
    )

    health = ServiceHealthManager(
        controller
    )

    result = await health.check_service(
        "svc-1"
    )

    assert result == "recovered"

    assert (
        controller.calls[-1][0]
        == "handle_service_failure"
    )


@pytest.mark.asyncio
async def test_health_handles_missing_runtime_service():
    controller = FakeController()

    async def not_found_status(**kwargs):
        return SimpleNamespace(
            payload={
                "status": "not_found",
            }
        )

    controller.status_service = (
        not_found_status
    )

    health = ServiceHealthManager(
        controller
    )

    result = await health.check_service(
        "svc-1"
    )

    assert result == "stopped"

    assert (
        controller.service_registry.service.status
        == "stopped"
    )