"""Controller restart recovery coordination for NodeForge."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from freemesh.controller.controller import Controller


class ControllerRecoveryManager:
    """Coordinate recovery after a Controller restart."""

    def __init__(
        self,
        controller: "Controller",
    ) -> None:
        if controller is None:
            raise TypeError(
                "controller is required"
            )

        self.controller = controller

    async def recover(self) -> list:
        """Reconcile all persisted service intents."""

        return await (
            self.controller.reconcile_all_services()
        )

    async def recover_service(
        self,
        service_id: str,
    ):
        """Reconcile one persisted service."""

        return await (
            self.controller.reconcile_service(
                service_id
            )
        )