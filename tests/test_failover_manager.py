from freemesh.controller.failover_manager import FailoverManager
from freemesh.scheduler.scheduler import NodeCandidate


def test_failover_manager_selects_replacement_node():
    manager = FailoverManager()

    replacement = manager.select_replacement_node(
        [
            NodeCandidate(
                node_id="node-a",
                available=True,
                running_services=1,
            ),
            NodeCandidate(
                node_id="node-b",
                available=True,
                running_services=2,
            ),
        ],
        failed_node_id="node-a",
    )

    assert replacement is not None
    assert replacement.node_id == "node-b"


def test_failover_manager_returns_none_without_replacement():
    manager = FailoverManager()

    replacement = manager.select_replacement_node(
        [
            NodeCandidate(
                node_id="node-a",
                available=True,
            ),
        ],
        failed_node_id="node-a",
    )

    assert replacement is None