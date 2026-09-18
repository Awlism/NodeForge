"""Tests for persistent service registry metadata."""

from freemesh.controller.service_metadata_store import (
    ServiceMetadataStore,
)
from freemesh.controller.service_registry import (
    ServiceRegistry,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


def test_registry_persists_registered_service(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceMetadataStore(
        str(database_path)
    )

    registry = ServiceRegistry(
        metadata_store=store
    )

    try:
        registry.register_service(
            service_id="bot-service",
            node_id="node-a",
            status="running",
            pid=1234,
            command="python3 bot.py",
            requirements=ServiceRequirements(
                cpu_cores=1.5,
                memory_mb=512,
                disk_gb=2.0,
            ),
        )

        stored = store.get("bot-service")

        assert stored is not None
        assert stored["service_id"] == "bot-service"
        assert stored["node_id"] == "node-a"
        assert stored["pid"] == 1234
        assert stored["status"] == "running"
        assert stored["command"] == "python3 bot.py"
        assert (
            stored["requirements"].memory_mb
            == 512
        )
    finally:
        store.close()


def test_registry_persists_service_updates(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceMetadataStore(
        str(database_path)
    )

    registry = ServiceRegistry(
        metadata_store=store
    )

    try:
        registry.register_service(
            service_id="bot-service",
            node_id="node-a",
            status="running",
            pid=100,
            command="python3 old.py",
            requirements=ServiceRequirements(),
        )

        registry.update_service(
            service_id="bot-service",
            status="crashed",
            pid=200,
            node_id="node-b",
            command="python3 new.py",
            requirements=ServiceRequirements(
                cpu_cores=2.0,
                memory_mb=1024,
                disk_gb=5.0,
            ),
        )

        stored = store.get("bot-service")

        assert stored is not None
        assert stored["node_id"] == "node-b"
        assert stored["pid"] == 200
        assert stored["status"] == "crashed"
        assert stored["command"] == "python3 new.py"
        assert (
            stored["requirements"].memory_mb
            == 1024
        )
    finally:
        store.close()


def test_registry_loads_persisted_services(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    first_store = ServiceMetadataStore(
        str(database_path)
    )

    first_registry = ServiceRegistry(
        metadata_store=first_store
    )

    first_registry.register_service(
        service_id="persistent-service",
        node_id="node-a",
        status="running",
        pid=5678,
        command="python3 app.py",
        requirements=ServiceRequirements(
            cpu_cores=2.0,
            memory_mb=1024,
            disk_gb=5.0,
        ),
    )

    first_store.close()

    second_store = ServiceMetadataStore(
        str(database_path)
    )

    second_registry = ServiceRegistry(
        metadata_store=second_store
    )

    try:
        loaded = second_registry.get_service(
            "persistent-service"
        )

        assert loaded is not None
        assert loaded.node_id == "node-a"
        assert loaded.pid == 5678
        assert loaded.status == "running"
        assert (
            loaded.requirements.memory_mb
            == 1024
        )
    finally:
        second_store.close()


def test_registry_delete_persists(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    store = ServiceMetadataStore(
        str(database_path)
    )

    registry = ServiceRegistry(
        metadata_store=store
    )

    try:
        registry.register_service(
            service_id="bot-service",
            node_id="node-a",
            status="running",
            pid=123,
            command="python3 bot.py",
            requirements=ServiceRequirements(),
        )

        registry.remove_service(
            "bot-service"
        )

        assert registry.get_service(
            "bot-service"
        ) is None

        assert not store.exists(
            "bot-service"
        )
    finally:
        store.close()