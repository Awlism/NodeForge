import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.service_requirements import ServiceRequirements


def test_controller_creates_service_intent():
    controller = Controller()

    intent = controller.create_service_intent(
        service_id="service-1",
        command="python worker.py",
    )

    assert intent.service_id == "service-1"
    assert intent.command == "python worker.py"
    assert intent.desired_state == DesiredState.RUNNING

    stored = controller.get_service_intent("service-1")

    assert stored is intent
    assert stored.service_id == "service-1"


def test_controller_creates_intent_with_requirements():
    controller = Controller()

    requirements = ServiceRequirements(
        cpu_cores=2.0,
        memory_mb=512,
        disk_gb=5.0,
    )

    intent = controller.create_service_intent(
        service_id="service-2",
        command="python app.py",
        requirements=requirements,
    )

    assert intent.requirements == requirements
    assert intent.requirements.cpu_cores == 2.0
    assert intent.requirements.memory_mb == 512
    assert intent.requirements.disk_gb == 5.0


def test_controller_changes_desired_state():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-3",
        command="python app.py",
    )

    intent = controller.set_service_desired_state(
        service_id="service-3",
        desired_state=DesiredState.STOPPED,
    )

    assert intent.desired_state == DesiredState.STOPPED

    stored = controller.get_service_intent("service-3")

    assert stored is not None
    assert stored.desired_state == DesiredState.STOPPED


def test_controller_removes_service_intent():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-4",
        command="python app.py",
    )

    removed = controller.remove_service_intent(
        "service-4"
    )

    assert removed.service_id == "service-4"
    assert controller.get_service_intent("service-4") is None


def test_controller_can_create_stopped_intent():
    controller = Controller()

    intent = controller.create_service_intent(
        service_id="service-5",
        command="python app.py",
        desired_state=DesiredState.STOPPED,
    )

    assert intent.service_id == "service-5"
    assert intent.command == "python app.py"
    assert intent.desired_state == DesiredState.STOPPED


def test_controller_rejects_duplicate_intent():
    controller = Controller()

    controller.create_service_intent(
        service_id="service-6",
        command="python app.py",
    )

    with pytest.raises(ValueError):
        controller.create_service_intent(
            service_id="service-6",
            command="python another.py",
        )


def test_controller_returns_none_for_missing_intent():
    controller = Controller()

    assert (
        controller.get_service_intent("missing-service")
        is None
    )


def test_controller_remove_missing_intent_raises():
    controller = Controller()

    with pytest.raises(KeyError):
        controller.remove_service_intent(
            "missing-service"
        )


def test_controller_changes_intent_without_changing_command():
    controller = Controller()

    intent = controller.create_service_intent(
        service_id="service-7",
        command="python original.py",
    )

    controller.set_service_desired_state(
        service_id="service-7",
        desired_state=DesiredState.STOPPED,
    )

    assert intent.command == "python original.py"
    assert intent.desired_state == DesiredState.STOPPED


def test_controller_intent_registry_is_independent():
    controller = Controller()

    intent = controller.create_service_intent(
        service_id="service-8",
        command="python app.py",
    )

    assert controller.service_intent_registry.exists(
        "service-8"
    )

    assert (
        controller.service_intent_registry.count()
        == 1
    )

    assert (
        controller.service_intent_registry.get(
            "service-8"
        )
        is intent
    )