"""Tests for persistent service metadata storage."""

from datetime import datetime, timezone

from freemesh.controller.service_metadata_store import (
    ServiceMetadataStore,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


def test_store_starts_empty(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceMetadataStore(
        str(database_path)
    )

    try:
        assert store.count() == 0
        assert store.list_all() == []
    finally:
        store.close()


def test_store_saves_and_loads_metadata(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceMetadataStore(
        str(database_path)
    )

    metadata = {
        "service_id": "bot-service",
        "node_id": "node-a",
        "pid": 1234,
        "status": "running",
        "health": "healthy",
        "command": "python3 bot.py",
        "requirements": ServiceRequirements(
            cpu_cores=1.5,
            memory_mb=512,
            disk_gb=2.0,
        ),
        "restart_attempts": 2,
    }

    try:
        store.save(**metadata)

        loaded = store.get(
            "bot-service"
        )

        assert loaded is not None
        assert (
            loaded["service_id"]
            == "bot-service"
        )
        assert loaded["node_id"] == "node-a"
        assert loaded["pid"] == 1234
        assert loaded["status"] == "running"
        assert loaded["health"] == "healthy"
        assert (
            loaded["command"]
            == "python3 bot.py"
        )
        assert (
            loaded["requirements"]
            == metadata["requirements"]
        )
        assert (
            loaded["restart_attempts"]
            == 2
        )
    finally:
        store.close()


def test_store_persists_across_connections(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    first_store = ServiceMetadataStore(
        str(database_path)
    )

    first_store.save(
        service_id="persistent-service",
        node_id="node-a",
        pid=5678,
        status="running",
        health="healthy",
        command="python3 app.py",
        requirements=ServiceRequirements(
            cpu_cores=2.0,
            memory_mb=1024,
            disk_gb=5.0,
        ),
        restart_attempts=1,
    )

    first_store.close()

    second_store = ServiceMetadataStore(
        str(database_path)
    )

    try:
        loaded = second_store.get(
            "persistent-service"
        )

        assert loaded is not None
        assert (
            loaded["node_id"]
            == "node-a"
        )
        assert loaded["pid"] == 5678
        assert (
            loaded["requirements"].memory_mb
            == 1024
        )
    finally:
        second_store.close()


def test_store_updates_metadata(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceMetadataStore(
        str(database_path)
    )

    try:
        store.save(
            service_id="bot-service",
            node_id="node-a",
            pid=100,
            status="running",
            health="healthy",
            command="python3 old.py",
            requirements=ServiceRequirements(),
            restart_attempts=0,
        )

        store.save(
            service_id="bot-service",
            node_id="node-b",
            pid=200,
            status="running",
            health="unhealthy",
            command="python3 new.py",
            requirements=ServiceRequirements(
                cpu_cores=2.0,
                memory_mb=2048,
                disk_gb=10.0,
            ),
            restart_attempts=3,
        )

        assert store.count() == 1

        loaded = store.get(
            "bot-service"
        )

        assert loaded is not None
        assert loaded["node_id"] == "node-b"
        assert loaded["pid"] == 200
        assert loaded["health"] == "unhealthy"
        assert (
            loaded["command"]
            == "python3 new.py"
        )
        assert (
            loaded["restart_attempts"]
            == 3
        )
        assert (
            loaded["requirements"].memory_mb
            == 2048
        )
    finally:
        store.close()


def test_store_lists_metadata(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceMetadataStore(
        str(database_path)
    )

    try:
        store.save(
            service_id="service-b",
            node_id="node-b",
            pid=2,
            status="running",
            health="healthy",
            command="python3 b.py",
            requirements=ServiceRequirements(),
            restart_attempts=0,
        )

        store.save(
            service_id="service-a",
            node_id="node-a",
            pid=1,
            status="running",
            health="healthy",
            command="python3 a.py",
            requirements=ServiceRequirements(),
            restart_attempts=0,
        )

        metadata = store.list_all()

        assert len(metadata) == 2
        assert (
            metadata[0]["service_id"]
            == "service-a"
        )
        assert (
            metadata[1]["service_id"]
            == "service-b"
        )
    finally:
        store.close()


def test_store_delete(tmp_path):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceMetadataStore(
        str(database_path)
    )

    try:
        store.save(
            service_id="bot-service",
            node_id="node-a",
            pid=123,
            status="running",
            health="healthy",
            command="python3 bot.py",
            requirements=ServiceRequirements(),
            restart_attempts=0,
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


def test_store_preserves_timestamp(
    tmp_path,
):
    database_path = (
        tmp_path / "nodeforge.db"
    )

    store = ServiceMetadataStore(
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

    try:
        store.save(
            service_id="time-service",
            node_id="node-a",
            pid=123,
            status="running",
            health="healthy",
            command="python3 app.py",
            requirements=ServiceRequirements(),
            restart_attempts=0,
            updated_at=timestamp,
        )

        loaded = store.get(
            "time-service"
        )

        assert loaded is not None
        assert (
            loaded["updated_at"]
            == timestamp
        )
        assert (
            loaded["updated_at"].tzinfo
            is not None
        )
    finally:
        store.close()