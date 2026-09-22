"""Service health monitoring and self-healing coordination."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from freemesh.controller.controller import Controller


class ServiceHealthManager:
    """Coordinate service health checks and self-healing."""

    def __init__(
        self,
        controller: "Controller",
    ) -> None:
        if controller is None:
            raise TypeError("controller is required")

        self.controller = controller

    async def check_service(
        self,
        service_id: str,
    ):
        """Check one service and trigger the existing failure path."""

        controller = self.controller

        service = controller.service_registry.get_service(
            service_id
        )

        if service is None:
            return None

        if service.status in {
            "stopped",
            "failed",
        }:
            return None

        node_id = service.node_id

        transport = controller._active_nodes.get(
            node_id
        )

        # If the service owner has no active transport,
        # use the existing failure/recovery path.
        if transport is None:
            failure_message = (
                controller._build_service_failure_message(
                    service=service,
                    status="failed",
                    error=(
                        "Health monitor detected "
                        "missing node transport"
                    ),
                )
            )

            return await controller._handle_service_failure(
                node_id=node_id,
                message=failure_message,
            )

        try:
            response = await controller.status_service(
                node_id=node_id,
                service_id=service_id,
                update_registry=False,
            )

        except Exception:
            return None

        if response is None:
            return None

        payload = getattr(
            response,
            "payload",
            None,
        )

        if not isinstance(payload, dict):
            return None

        runtime_status = payload.get(
            "status"
        )

        if runtime_status in {
            "not_found",
            "stopped",
        }:
            current_service = (
                controller.service_registry.get_service(
                    service_id
                )
            )

            if (
                current_service is not None
                and current_service.node_id == node_id
            ):
                controller.service_registry.update_service(
                    service_id=service_id,
                    status="stopped",
                    pid=None,
                )

            return runtime_status

        if runtime_status in {
            "crashed",
            "failed",
        }:
            failure_message = (
                controller._build_service_failure_message(
                    service=service,
                    status=runtime_status,
                    error=(
                        "Health monitor detected "
                        "runtime status: "
                        f"{runtime_status}"
                    ),
                    restart_attempts=payload.get(
                        "restart_attempts",
                        0,
                    ),
                )
            )

            return await controller._handle_service_failure(
                node_id=node_id,
                message=failure_message,
            )

        return runtime_status

    async def monitor_once(
        self,
    ) -> None:
        """Run one health-monitoring pass over all services."""

        controller = self.controller

        services = (
            controller.service_registry.list_services()
        )

        for service in services:
            try:
                await self.check_service(
                    service.service_id
                )

            except (
                RuntimeError,
                TimeoutError,
                KeyError,
                ValueError,
            ):
                continue

            except Exception:
                # A single unhealthy service must never stop
                # monitoring the remaining services.
                continue