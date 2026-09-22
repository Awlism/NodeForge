from freemesh.controller.controller import Controller
from freemesh.controller.mvp_validator import (
    MVPValidator,
)


def test_mvp_validator_accepts_current_controller():
    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
    )

    validator = MVPValidator(
        controller
    )

    assert validator.is_ready() is True

    summary = validator.summary()

    assert summary["ready"] is True
    assert summary["failed"] == 0
    assert summary["passed"] > 0


def test_mvp_validator_reports_missing_capability():
    class MinimalController:
        pass

    validator = MVPValidator(
        MinimalController()
    )

    assert validator.is_ready() is False

    summary = validator.summary()

    assert summary["ready"] is False
    assert summary["failed"] > 0