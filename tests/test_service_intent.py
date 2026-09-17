"""Tests for NodeForge service desired state."""

from datetime import datetime, timezone

import pytest

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


def test_service_intent_defaults_to_running_requirements():
    intent = ServiceIntent(
        service_id="test-service",
        desired_state=DesiredState.RUNNING,
    )

    assert intent.service_id == "test-service"
    assert (
        intent.desired_state
        == DesiredState.RUNNING
    )
    assert intent.command is None
    assert (
        intent.requirements
        == ServiceRequirements()
    )
    assert isinstance(
        intent.updated_at,
        datetime,
    )


def test_service_intent_accepts_command_and_requirements():
    requirements = ServiceRequirements(
        cpu_cores=1.5,
        memory_mb=512,
        disk_gb=2.0,
    )

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
        command="python3 bot.py",
        requirements=requirements,
    )

    assert intent.command == "python3 bot.py"
    assert (
        intent.requirements
        == requirements
    )


def test_service_intent_can_change_desired_state():
    intent = ServiceIntent(
        service_id="test-service",
        desired_state=DesiredState.RUNNING,
    )

    old_updated_at = intent.updated_at

    intent.set_desired_state(
        DesiredState.STOPPED
    )

    assert (
        intent.desired_state
        == DesiredState.STOPPED
    )
    assert intent.updated_at >= old_updated_at


def test_service_intent_update_changes_configuration():
    intent = ServiceIntent(
        service_id="test-service",
        desired_state=DesiredState.STOPPED,
    )

    requirements = ServiceRequirements(
        cpu_cores=2.0,
        memory_mb=1024,
        disk_gb=5.0,
    )

    intent.update(
        desired_state=DesiredState.RUNNING,
        command="python3 app.py",
        requirements=requirements,
    )

    assert (
        intent.desired_state
        == DesiredState.RUNNING
    )
    assert intent.command == "python3 app.py"
    assert (
        intent.requirements
        == requirements
    )


def test_service_intent_requires_service_id():
    with pytest.raises(ValueError):
        ServiceIntent(
            service_id="",
            desired_state=DesiredState.RUNNING,
        )


def test_service_intent_rejects_invalid_desired_state():
    with pytest.raises(TypeError):
        ServiceIntent(
            service_id="test-service",
            desired_state="running",
        )


def test_service_intent_rejects_invalid_requirements():
    with pytest.raises(TypeError):
        ServiceIntent(
            service_id="test-service",
            desired_state=DesiredState.RUNNING,
            requirements={},  # type: ignore[arg-type]
        )


def test_service_intent_rejects_empty_command():
    with pytest.raises(ValueError):
        ServiceIntent(
            service_id="test-service",
            desired_state=DesiredState.RUNNING,
            command="",
        )