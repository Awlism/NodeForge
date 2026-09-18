"""Tests for Controller service metadata persistence."""

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.service_requirements import ServiceRequirements


def test_controller_persists_service_metadata(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    controller = Controller(
        database_path=str(database_path)
    )

    try:
        controller.service_registry.register_service(
            service_id="persistent-service",
            node_id="node-a",
            status="running",
            pid=1234,
            command="python3 app.py",
            requirements=ServiceRequirements(
                cpu_cores=1.0,
                memory_mb=512,
                disk_gb=1.0,
            ),
        )

        stored = controller.service_metadata_store.get(
            "persistent-service"
        )

        assert stored is not None
        assert stored["service_id"] == "persistent-service"
        assert stored["node_id"] == "node-a"
        assert stored["pid"] == 1234
        assert stored["status"] == "running"

    finally:
        controller.close()


def test_controller_recovers_service_metadata_after_restart(
    tmp_path,
):
    database_path = tmp_path / "nodeforge.db"

    controller_one = Controller(
        database_path=str(database_path)
    )

    controller_one.service_registry.register_service(
        service_id="recovered-service",
        node_id="node-a",
        status="running",
        pid=5678,
        command="python3 app.py",
        requirements=ServiceRequirements(
            cpu_cores=2.0,
            memory_mb=1024,
            disk_gb=2.0,
        ),
    )

    controller_one.close()

    controller_two = Controller(
        database_path=str(database_path)
    )

    try:
        service = controller_two.service_registry.get_service(
            "recovered-service"
        )

        assert service is not None
        assert service.service_id == "recovered-service"
        assert service.node_id == "node-a"
        assert service.pid == 5678
        assert service.status == "running"
        assert service.command == "python3 app.py"
        assert service.requirements.cpu_cores == 2.0
        assert service.requirements.memory_mb == 1024
        assert service.requirements.disk_gb == 2.0

    finally:
        controller_two.close()