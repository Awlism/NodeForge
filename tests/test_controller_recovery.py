import pytest

from freemesh.controller.controller_recovery import (
    ControllerRecoveryManager,
)


class FakeController:
    def __init__(self):
        self.calls = []

    async def reconcile_all_services(self):
        self.calls.append(
            "reconcile_all"
        )
        return ["result"]

    async def reconcile_service(
        self,
        service_id,
    ):
        self.calls.append(
            ("reconcile", service_id)
        )
        return "result"


@pytest.mark.asyncio
async def test_controller_recovery_all():
    controller = FakeController()

    manager = ControllerRecoveryManager(
        controller
    )

    result = await manager.recover()

    assert result == ["result"]
    assert controller.calls == [
        "reconcile_all"
    ]


@pytest.mark.asyncio
async def test_controller_recovery_one_service():
    controller = FakeController()

    manager = ControllerRecoveryManager(
        controller
    )

    result = await manager.recover_service(
        "svc-1"
    )

    assert result == "result"

    assert controller.calls == [
        ("reconcile", "svc-1")
    ]