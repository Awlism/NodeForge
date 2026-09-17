from freemesh.node.agent import NodeAgent
from freemesh.protocol.messages import MessageType


def test_agent_resource_payload_contains_node_information():
    agent = NodeAgent(
        node_id="node-test",
    )

    payload = agent._collect_resource_payload()

    assert payload["node_id"] == "node-test"

    assert payload["cpu_cores"] >= 1
    assert 0 <= payload["cpu_usage_percent"] <= 100

    assert payload["memory_total_mb"] >= 0
    assert payload["memory_used_mb"] >= 0

    assert payload["disk_total_gb"] >= 0
    assert payload["disk_used_gb"] >= 0

    assert payload["running_services"] == 0


def test_resource_report_message_type_exists():
    assert MessageType.RESOURCE_REPORT.value == (
        "resource_report"
    )


def test_resource_report_response_message_type_exists():
    assert MessageType.RESOURCE_REPORT_RESPONSE.value == (
        "resource_report_response"
    )