"""Tests for Controller service desired state."""

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.service_requirements import ServiceRequirements


def test_controller_creates_service_intent():
    controller = Controller()

    intent = controller.create_service_intent(
        service_id="bot-service",
        command="python3 bot.py",
    )

    assert intent.service_id == "bot-service"
    assert intent.command == "python3 bot.py"
    assert (
        intent.desired_state
        == DesiredState.RUNNING
    )

    assert (
        controller.get_service_intent("bot-service")
        is intent
    )


def test_controller_creates_intent_with_requirements():
    controller = Controller()

    requirements = ServiceRequirements(
        cpu_cores=1.5,
        memory_mb=512,
        disk_gb=2.0,
    )

    intent = controller.create_service_intent(
        service_id="bot-service",
        command="python3 bot.py",
        requirements=requirements,
    )

    assert intent.requirements == requirements


def test_controller_changes_desired_state():
    controller = Controller()

    controller.create_service_intent(
        service_id="bot-service",
        command="python3 bot.py",
    )

    intent = controller.set_service_desired_state(
        "bot-service",
        DesiredState.STOPPED,
    )

    assert (
        intent.desired_state
        == DesiredState.STOPPED
    )


def test_controller_removes_service_intent():
    controller = Controller()

    controller.create_service_intent(
        service_id="bot-service",
        command="python3 bot.py",
    )

    removed = controller.remove_service_intent(
        "bot-service"
    )

    assert removed.service_id == "bot-service"
    assert (
        controller.get_service_intent("bot-service")
        is None
    )


def test_controller_can_create_stopped_intent():
    controller = Controller()

    intent = controller.create_service_intent(
        service_id="bot-service",
        command="python3 bot.py",
        desired_state=DesiredState.STOPPED,
    )

    assert (
        intent.desired_state
        == DesiredState.STOPPED
    )