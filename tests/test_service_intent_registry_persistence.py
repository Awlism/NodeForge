"""Tests for persistent ServiceIntentRegistry integration."""

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.controller.service_intent_registry import (
    ServiceIntentRegistry,
)
from freemesh.controller.service_intent_store import (
    ServiceIntentStore,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


def test_registry_can_load_persisted_intents(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceIntentStore(str(database_path))

    store.save(
        ServiceIntent(
            service_id="bot-service",
            desired_state=DesiredState.RUNNING,
            command="python3 bot.py",
            requirements=ServiceRequirements(
                cpu_cores=1.0,
                memory_mb=512,
                disk_gb=2.0,
            ),
        )
    )

    store.close()

    registry = ServiceIntentRegistry()

    registry.load_from_store(
        ServiceIntentStore(str(database_path))
    )

    intent = registry.get("bot-service")

    assert intent is not None
    assert intent.desired_state == DesiredState.RUNNING
    assert intent.command == "python3 bot.py"
    assert intent.requirements.memory_mb == 512


def test_registry_register_persists_intent(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceIntentStore(str(database_path))
    registry = ServiceIntentRegistry(store=store)

    intent = ServiceIntent(
        service_id="persistent-service",
        desired_state=DesiredState.RUNNING,
        command="python3 app.py",
    )

    registry.register(intent)

    store.close()

    second_store = ServiceIntentStore(
        str(database_path)
    )

    loaded = second_store.get(
        "persistent-service"
    )

    assert loaded is not None
    assert loaded.command == "python3 app.py"

    second_store.close()


def test_registry_update_persists_intent(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceIntentStore(str(database_path))
    registry = ServiceIntentRegistry(store=store)

    registry.register(
        ServiceIntent(
            service_id="bot-service",
            desired_state=DesiredState.RUNNING,
            command="python3 old.py",
        )
    )

    registry.update(
        service_id="bot-service",
        desired_state=DesiredState.STOPPED,
        command="python3 new.py",
    )

    store.close()

    second_store = ServiceIntentStore(
        str(database_path)
    )

    loaded = second_store.get(
        "bot-service"
    )

    assert loaded is not None
    assert loaded.desired_state == DesiredState.STOPPED
    assert loaded.command == "python3 new.py"

    second_store.close()


def test_registry_remove_persists_deletion(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceIntentStore(str(database_path))
    registry = ServiceIntentRegistry(store=store)

    registry.register(
        ServiceIntent(
            service_id="bot-service",
            desired_state=DesiredState.RUNNING,
        )
    )

    registry.remove("bot-service")

    assert not store.exists("bot-service")

    store.close()