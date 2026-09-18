import asyncio

import pytest

from freemesh.controller.controller import Controller
from freemesh.controller.service_intent import DesiredState
from freemesh.node.agent import NodeAgent
from freemesh.node.auth import DevelopmentTokenAuthenticator


@pytest.mark.asyncio
async def test_desired_state_starts_missing_service():
    controller = Controller()

    token = "nodeforge-e2e-token"

    # The real NodeAgent authentication uses the environment variable.
    # Keep this test isolated from any user-specific environment.
    import os

    os.environ["NODEFORGE_AUTH_TOKEN"] = token

    node = NodeAgent(
        node_id="desired-state-node",
        host="127.0.0.1",
        port=0,
        authenticator=DevelopmentTokenAuthenticator(),
    )

    await controller.start()

    try:
        await node.start()

        # Give the Controller/Node connection time to establish.
        await asyncio.sleep(0.2)

        controller.create_service_intent(
            service_id="desired-service",
            desired_state=DesiredState.RUNNING,
            command=(
                "python -c "
                "\"import time; time.sleep(30)\""
            ),
        )

        result = await controller.reconcile_service(
            "desired-service"
        )

        assert result.action == "start"
        assert result.changed is True

        # Allow the real SERVICE_START flow to complete.
        await asyncio.sleep(0.5)

        service = controller.service_registry.get_service(
            "desired-service"
        )

        assert service is not None
        assert service.node_id == "desired-state-node"

    finally:
        await node.stop()
        await controller.stop()