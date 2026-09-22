import pytest

from freemesh.controller.service_orchestrator import ServiceOrchestrator
from freemesh.service_requirements import ServiceRequirements


class FakeService:
    def __init__(self, node_id: str):
        self.node_id = node_id


class FakeRegistry:
    def __init__(self):
        self.services = {}

    def get_service(self, service_id: str):
        return self.services.get(service_id)


class FakeIntentRegistry:
    def list_all(self):
        return []


class FakeReconciler:
    async def reconcile(
        self,
        service_id,
        actual_service,
        start_service,
        stop_service,
        migrate_service,
    ):
        return type(
            "Result",
            (),
            {
                "service_id": service_id,
                "action": "noop",
                "changed": False,
                "reason": "test",
            },
        )()


class FakeController:
    def __init__(self):
        self.service_registry = FakeRegistry()
        self.service_intent_registry = FakeIntentRegistry()
        self.reconciler = FakeReconciler()

        self.calls = []

    async def start_service_auto(self, **kwargs):
        self.calls.append(("start", kwargs))
        return "started"

    async def stop_service(self, **kwargs):
        self.calls.append(("stop", kwargs))
        return "stopped"

    async def migrate_service(self, **kwargs):
        self.calls.append(("migrate", kwargs))
        return "migrated"


@pytest.mark.asyncio
async def test_ensure_running_delegates_to_controller():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(controller)

    requirements = ServiceRequirements(
        cpu_cores=1.0,
        memory_mb=512,
        disk_gb=2.0,
    )

    result = await orchestrator.ensure_running(
        service_id="svc-1",
        command="python app.py",
        requirements=requirements,
    )

    assert result == "started"
    assert controller.calls == [
        (
            "start",
            {
                "service_id": "svc-1",
                "command": "python app.py",
                "required_cpu_cores": 1.0,
                "required_memory_mb": 512,
                "required_disk_gb": 2.0,
            },
        )
    ]


@pytest.mark.asyncio
async def test_ensure_stopped_uses_registered_node():
    controller = FakeController()
    controller.service_registry.services["svc-1"] = FakeService("node-a")

    orchestrator = ServiceOrchestrator(controller)

    result = await orchestrator.ensure_stopped("svc-1")

    assert result == "stopped"
    assert controller.calls == [
        (
            "stop",
            {
                "node_id": "node-a",
                "service_id": "svc-1",
            },
        )
    ]


@pytest.mark.asyncio
async def test_recover_uses_registered_node():
    controller = FakeController()
    controller.service_registry.services["svc-1"] = FakeService("node-a")

    orchestrator = ServiceOrchestrator(controller)

    result = await orchestrator.recover("svc-1")

    assert result == "migrated"
    assert controller.calls == [
        (
            "migrate",
            {
                "service_id": "svc-1",
                "failed_node_id": "node-a",
            },
        )
    ]


@pytest.mark.asyncio
async def test_missing_service_is_safe():
    controller = FakeController()
    orchestrator = ServiceOrchestrator(controller)

    assert await orchestrator.ensure_stopped("missing") is None
    assert await orchestrator.recover("missing") is None
    assert controller.calls == []