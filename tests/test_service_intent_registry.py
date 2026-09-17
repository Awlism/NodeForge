"""Tests for the NodeForge service intent registry."""

import pytest

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.controller.service_intent_registry import (
    ServiceIntentRegistry,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


def test_registry_starts_empty():
    registry = ServiceIntentRegistry()

    assert registry.count() == 0
    assert registry.list_all() == []


def test_registry_registers_and_gets_intent():
    registry = ServiceIntentRegistry()

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
    )

    result = registry.register(intent)

    assert result is intent
    assert registry.get("bot-service") is intent
    assert registry.exists("bot-service") is True
    assert registry.count() == 1


def test_registry_rejects_duplicate_intent():
    registry = ServiceIntentRegistry()

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
    )

    registry.register(intent)

    with pytest.raises(ValueError):
        registry.register(intent)


def test_registry_requires_service_intent():
    registry = ServiceIntentRegistry()

    with pytest.raises(TypeError):
        registry.register({})  # type: ignore[arg-type]


def test_registry_require_returns_intent():
    registry = ServiceIntentRegistry()

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
    )

    registry.register(intent)

    assert registry.require("bot-service") is intent


def test_registry_require_raises_for_missing_intent():
    registry = ServiceIntentRegistry()

    with pytest.raises(KeyError):
        registry.require("missing-service")


def test_registry_updates_intent():
    registry = ServiceIntentRegistry()

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
    )

    registry.register(intent)

    requirements = ServiceRequirements(
        cpu_cores=2.0,
        memory_mb=1024,
        disk_gb=5.0,
    )

    updated = registry.update(
        "bot-service",
        desired_state=DesiredState.STOPPED,
        command="python3 bot.py",
        requirements=requirements,
    )

    assert updated is intent
    assert (
        intent.desired_state
        == DesiredState.STOPPED
    )
    assert intent.command == "python3 bot.py"
    assert intent.requirements == requirements


def test_registry_changes_desired_state():
    registry = ServiceIntentRegistry()

    registry.register(
        ServiceIntent(
            service_id="bot-service",
            desired_state=DesiredState.STOPPED,
        )
    )

    updated = registry.set_desired_state(
        "bot-service",
        DesiredState.RUNNING,
    )

    assert (
        updated.desired_state
        == DesiredState.RUNNING
    )


def test_registry_removes_intent():
    registry = ServiceIntentRegistry()

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
    )

    registry.register(intent)

    removed = registry.remove("bot-service")

    assert removed is intent
    assert registry.exists("bot-service") is False
    assert registry.count() == 0


def test_registry_remove_missing_raises():
    registry = ServiceIntentRegistry()

    with pytest.raises(KeyError):
        registry.remove("missing-service")


def test_registry_lists_all_intents():
    registry = ServiceIntentRegistry()

    first = ServiceIntent(
        service_id="service-a",
        desired_state=DesiredState.RUNNING,
    )

    second = ServiceIntent(
        service_id="service-b",
        desired_state=DesiredState.STOPPED,
    )

    registry.register(first)
    registry.register(second)

    intents = registry.list_all()

    assert len(intents) == 2
    assert first in intents
    assert second in intents


def test_registry_clear():
    registry = ServiceIntentRegistry()

    registry.register(
        ServiceIntent(
            service_id="service-a",
            desired_state=DesiredState.RUNNING,
        )
    )

    registry.register(
        ServiceIntent(
            service_id="service-b",
            desired_state=DesiredState.STOPPED,
        )
    )

    registry.clear()

    assert registry.count() == 0
    assert registry.list_all() == []