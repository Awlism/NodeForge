from freemesh.controller.controller import Controller
from freemesh.controller.node_registry import NodeRegistry, NodeState
from freemesh.node.agent import NodeAgent
from freemesh.protocol.messages import MessageType


def test_core_imports_and_construction():
    registry = NodeRegistry()

    controller = Controller(
        host="127.0.0.1",
        port=0,
        heartbeat_timeout_seconds=2.0,
    )

    agent = NodeAgent(
        node_id="test-node",
        hostname="test-host",
        controller_host="127.0.0.1",
        controller_port=9999,
        authentication_token="test-token",
    )

    assert registry.node_count() == 0
    assert controller.host == "127.0.0.1"
    assert controller.port == 0
    assert agent.get_state().value == "disconnected"


def test_message_types_exist():
    assert MessageType.REGISTER
    assert MessageType.REGISTER_RESPONSE
    assert MessageType.AUTHENTICATE
    assert MessageType.AUTHENTICATE_RESPONSE
    assert MessageType.HEARTBEAT
    assert MessageType.HEARTBEAT_RESPONSE


def test_node_registry_states_exist():
    assert NodeState.UNKNOWN
    assert NodeState.REGISTERING
    assert NodeState.ONLINE
    assert NodeState.OFFLINE
    assert NodeState.AUTH_FAILED