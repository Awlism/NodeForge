from freemesh.scheduler.scheduler import NodeCandidate, Scheduler


def test_scheduler_selects_least_loaded_node():
    scheduler = Scheduler()

    selected = scheduler.select_node(
        [
            NodeCandidate(
                node_id="node-1",
                running_services=5,
            ),
            NodeCandidate(
                node_id="node-2",
                running_services=2,
            ),
            NodeCandidate(
                node_id="node-3",
                running_services=8,
            ),
        ]
    )

    assert selected is not None
    assert selected.node_id == "node-2"


def test_scheduler_excludes_failed_node():
    scheduler = Scheduler()

    selected = scheduler.select_node(
        [
            NodeCandidate(
                node_id="node-1",
                running_services=0,
            ),
            NodeCandidate(
                node_id="node-2",
                running_services=3,
            ),
        ],
        exclude_node_id="node-1",
    )

    assert selected is not None
    assert selected.node_id == "node-2"


def test_scheduler_returns_none_without_available_nodes():
    scheduler = Scheduler()

    selected = scheduler.select_node(
        [
            NodeCandidate(
                node_id="node-1",
                available=False,
            ),
            NodeCandidate(
                node_id="node-2",
                available=False,
            ),
        ]
    )

    assert selected is None