import pytest

from freemesh.service_requirements import ServiceRequirements


def test_default_requirements_are_zero():
    requirements = ServiceRequirements()

    assert requirements.cpu_cores == 0.0
    assert requirements.memory_mb == 0
    assert requirements.disk_gb == 0.0


def test_requirements_are_stored():
    requirements = ServiceRequirements(
        cpu_cores=2.0,
        memory_mb=512,
        disk_gb=5.0,
    )

    assert requirements.cpu_cores == 2.0
    assert requirements.memory_mb == 512
    assert requirements.disk_gb == 5.0


def test_requirements_to_dict():
    requirements = ServiceRequirements(
        cpu_cores=1.5,
        memory_mb=1024,
        disk_gb=10.0,
    )

    assert requirements.to_dict() == {
        "cpu_cores": 1.5,
        "memory_mb": 1024,
        "disk_gb": 10.0,
    }


def test_requirements_from_dict():
    requirements = ServiceRequirements.from_dict(
        {
            "cpu_cores": 2,
            "memory_mb": 2048,
            "disk_gb": 20,
        }
    )

    assert requirements.cpu_cores == 2.0
    assert requirements.memory_mb == 2048
    assert requirements.disk_gb == 20.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cpu_cores": -1},
        {"memory_mb": -1},
        {"disk_gb": -1},
    ],
)
def test_negative_requirements_are_rejected(kwargs):
    with pytest.raises(ValueError):
        ServiceRequirements(**kwargs)