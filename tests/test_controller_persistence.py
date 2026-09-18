"""Tests for Controller persistent service intent recovery."""

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.service_requirements import ServiceRequirements


def test_controller_persists_service_intent(tmp_path):
    database_path = tmp_path / "nodeforge.db"

    controller = Controller(
        database_path=str(database_path)
    )

    controller.create_service_intent(
        service_id="persistent-service",
        command="python3 app.py",
        desired_state=DesiredState.RUNNING,
        requirements=ServiceRequirements(
            cpu_cores=1.0,
            memory_mb=512,
            disk_gb=1.0,
        ),
    )

    controller.close()


def test_controller_recovers_service_intent_after_restart(
    tmp_path,
):
    database_path = tmp_path / "nodeforge.db"

    controller_one = Controller(
        database_path=str(database_path)
    )

    controller_one.create_service_intent(
        service_id="recovered-service",
        command="python3 app.py",
        desired_state=DesiredState.RUNNING,
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

    intent = controller_two.get_service_intent(
        "recovered-service"
    )

    assert intent is not None
    assert intent.service_id == "recovered-service"
    assert intent.desired_state == DesiredState.RUNNING
    assert intent.command == "python3 app.py"
    assert intent.requirements.cpu_cores == 2.0
    assert intent.requirements.memory_mb == 1024
    assert intent.requirements.disk_gb == 2.0

    controller_two.close()