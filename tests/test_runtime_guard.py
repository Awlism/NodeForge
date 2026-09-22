from freemesh.controller.runtime_guard import RuntimeGuard


def test_runtime_guard_accepts_valid_service_id():
    guard = RuntimeGuard()

    result = guard.validate_service_id(
        "arzankadeh"
    )

    assert result.allowed is True


def test_runtime_guard_rejects_empty_service_id():
    guard = RuntimeGuard()

    result = guard.validate_service_id("")

    assert result.allowed is False
    assert result.reason == "service_id_required"


def test_runtime_guard_rejects_non_string_service_id():
    guard = RuntimeGuard()

    result = guard.validate_service_id(
        123
    )

    assert result.allowed is False
    assert (
        result.reason
        == "service_id_must_be_string"
    )


def test_runtime_guard_accepts_command():
    guard = RuntimeGuard()

    result = guard.validate_command(
        "python worker.py"
    )

    assert result.allowed is True


def test_runtime_guard_rejects_empty_command():
    guard = RuntimeGuard()

    result = guard.validate_command("")

    assert result.allowed is False
    assert result.reason == "command_required"


def test_runtime_guard_accepts_positive_timeout():
    guard = RuntimeGuard()

    result = guard.validate_timeout(
        10.0
    )

    assert result.allowed is True


def test_runtime_guard_rejects_non_positive_timeout():
    guard = RuntimeGuard()

    assert (
        guard.validate_timeout(0).allowed
        is False
    )

    assert (
        guard.validate_timeout(-1).allowed
        is False
    )