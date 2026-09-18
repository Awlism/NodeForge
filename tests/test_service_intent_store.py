"""Tests for persistent service intent storage."""

from datetime import datetime, timezone

from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.controller.service_intent_store import (
    ServiceIntentStore,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


def test_store_starts_empty(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceIntentStore(
        str(database_path)
    )

    try:
        assert store.count() == 0
        assert store.list_all() == []
    finally:
        store.close()


def test_store_saves_and_loads_intent(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceIntentStore(
        str(database_path)
    )

    intent = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
        command="python3 bot.py",
        requirements=ServiceRequirements(
            cpu_cores=1.5,
            memory_mb=512,
            disk_gb=2.0,
        ),
    )

    try:
        store.save(intent)

        loaded = store.get(
            "bot-service"
        )

        assert loaded is not None
        assert (
            loaded.service_id
            == intent.service_id
        )
        assert (
            loaded.desired_state
            == DesiredState.RUNNING
        )
        assert (
            loaded.command
            == "python3 bot.py"
        )
        assert (
            loaded.requirements
            == intent.requirements
        )
        assert (
            loaded.updated_at
            == intent.updated_at
        )
    finally:
        store.close()


def test_store_persists_across_connections(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    intent = ServiceIntent(
        service_id="persistent-service",
        desired_state=DesiredState.RUNNING,
        command="python3 app.py",
        requirements=ServiceRequirements(
            cpu_cores=2.0,
            memory_mb=1024,
            disk_gb=5.0,
        ),
    )

    first_store = ServiceIntentStore(
        str(database_path)
    )

    first_store.save(intent)
    first_store.close()

    second_store = ServiceIntentStore(
        str(database_path)
    )

    try:
        loaded = second_store.get(
            "persistent-service"
        )

        assert loaded is not None
        assert (
            loaded.service_id
            == "persistent-service"
        )
        assert (
            loaded.command
            == "python3 app.py"
        )
        assert (
            loaded.requirements.memory_mb
            == 1024
        )
    finally:
        second_store.close()


def test_store_updates_existing_intent(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceIntentStore(
        str(database_path)
    )

    first = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.RUNNING,
        command="python3 old.py",
    )

    second = ServiceIntent(
        service_id="bot-service",
        desired_state=DesiredState.STOPPED,
        command="python3 new.py",
        requirements=ServiceRequirements(
            cpu_cores=2.0,
            memory_mb=2048,
            disk_gb=10.0,
        ),
    )

    try:
        store.save(first)
        store.save(second)

        assert store.count() == 1

        loaded = store.get(
            "bot-service"
        )

        assert loaded is not None
        assert (
            loaded.desired_state
            == DesiredState.STOPPED
        )
        assert (
            loaded.command
            == "python3 new.py"
        )
        assert (
            loaded.requirements.memory_mb
            == 2048
        )
    finally:
        store.close()


def test_store_lists_all_intents(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceIntentStore(
        str(database_path)
    )

    try:
        store.save(
            ServiceIntent(
                service_id="service-b",
                desired_state=DesiredState.STOPPED,
            )
        )

        store.save(
            ServiceIntent(
                service_id="service-a",
                desired_state=DesiredState.RUNNING,
            )
        )

        intents = store.list_all()

        assert len(intents) == 2
        assert (
            intents[0].service_id
            == "service-a"
        )
        assert (
            intents[1].service_id
            == "service-b"
        )
    finally:
        store.close()


def test_store_delete(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceIntentStore(
        str(database_path)
    )

    try:
        store.save(
            ServiceIntent(
                service_id="bot-service",
                desired_state=DesiredState.RUNNING,
            )
        )

        assert store.exists(
            "bot-service"
        )

        assert store.delete(
            "bot-service"
        ) is True

        assert not store.exists(
            "bot-service"
        )

        assert store.delete(
            "bot-service"
        ) is False
    finally:
        store.close()


def test_store_preserves_timezone(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceIntentStore(
        str(database_path)
    )

    timestamp = datetime(
        2026,
        1,
        1,
        12,
        30,
        tzinfo=timezone.utc,
    )

    intent = ServiceIntent(
        service_id="time-service",
        desired_state=DesiredState.RUNNING,
        updated_at=timestamp,
    )

    try:
        store.save(intent)

        loaded = store.get(
            "time-service"
        )

        assert loaded is not None
        assert (
            loaded.updated_at
            == timestamp
        )
        assert (
            loaded.updated_at.tzinfo
            is not None
        )
    finally:
        store.close()