"""Controller for the NodeForge distributed system."""

import asyncio
import uuid
from typing import Dict, Optional

from freemesh.controller.failure_manager import FailureManager
from freemesh.controller.failover_manager import FailoverManager
from freemesh.controller.migration_manager import (
    MigrationManager,
)
from freemesh.controller.migration_registry import (
    MigrationRegistry,
)
from freemesh.controller.node_registry import (
    NodeRegistry,
    NodeState,
)
from freemesh.controller.resource_accounting import (
    ResourceAccounting,
)
from freemesh.controller.resource_failover import (
    ResourceFailover,
)
from freemesh.controller.resource_registry import (
    ResourceRegistry,
)
from freemesh.controller.service_placement import (
    ServicePlacement,
)
from freemesh.controller.service_registry import (
    ServiceRegistry,
)
from freemesh.node.resources import NodeResources
from freemesh.protocol.messages import (
    BaseMessage,
    MessageType,
)
from freemesh.protocol.transport import TCPTransport
from freemesh.scheduler.resource_scheduler import (
    ResourceNodeCandidate,
    ResourceScheduler,
)
from freemesh.scheduler.scheduler import NodeCandidate
from freemesh.security.auth import (
    Authenticator,
    AuthenticationError,
)
from freemesh.service_requirements import (
    ServiceRequirements,
)


class Controller:
    """Central controller for managing NodeForge nodes."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9999,
        heartbeat_timeout_seconds: float = 30.0,
        authenticator: Optional[Authenticator] = None,
    ):
        self.host = host
        self.port = port
        self.heartbeat_timeout_seconds = (
            heartbeat_timeout_seconds
        )
        self.authenticator = authenticator

        self.registry = NodeRegistry()
        self.resource_registry = ResourceRegistry()
        self.service_registry = ServiceRegistry()
        self.failure_manager = FailureManager()

        self.failover_manager = FailoverManager()
        self.resource_scheduler = ResourceScheduler()

        self.service_placement = ServicePlacement(
            scheduler=self.resource_scheduler,
        )

        self.resource_failover = ResourceFailover(
            scheduler=self.resource_scheduler,
        )

        self.resource_accounting = ResourceAccounting()

        self.migration_manager = MigrationManager(
            accounting=self.resource_accounting,
        )

        self.migration_registry = MigrationRegistry()

        self.server: Optional[asyncio.Server] = None
        self._running = False

        self._offline_detection_task: Optional[
            asyncio.Task
        ] = None

        self._service_health_task: Optional[
            asyncio.Task
        ] = None

        self.service_health_interval_seconds = 2.0

        self._active_nodes: Dict[
            str,
            TCPTransport,
        ] = {}

        self._service_responses: Dict[
            str,
            BaseMessage,
        ] = {}

        self._service_response_events: Dict[
            str,
            asyncio.Event,
        ] = {}

    async def start(self) -> None:
        """Start the controller."""

        if self.server is not None:
            raise RuntimeError(
                "Controller is already running"
            )

        self._running = True

        self.server = await asyncio.start_server(
            self._handle_client_connection,
            self.host,
            self.port,
        )

        self._offline_detection_task = (
            asyncio.create_task(
                self._run_offline_detection()
            )
        )

        self._service_health_task = (
            asyncio.create_task(
                self._run_service_health_monitor()
            )
        )

        async with self.server:
            await self.server.serve_forever()

    async def stop(self) -> None:
        """Stop the controller and close all connections."""

        self._running = False

        if self._offline_detection_task:
            self._offline_detection_task.cancel()

            try:
                await self._offline_detection_task
            except asyncio.CancelledError:
                pass

            self._offline_detection_task = None

        if self._service_health_task:
            self._service_health_task.cancel()

            try:
                await self._service_health_task
            except asyncio.CancelledError:
                pass

            self._service_health_task = None

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        for transport in list(
            self._active_nodes.values()
        ):
            try:
                await transport.disconnect()
            except Exception:
                pass

        self._active_nodes.clear()

        for event in (
            self._service_response_events.values()
        ):
            event.set()

        self._service_response_events.clear()
        self._service_responses.clear()

        self.resource_registry.clear()
        self.resource_accounting.clear()

    async def _handle_client_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle an incoming node connection."""

        transport = TCPTransport()
        transport.reader = reader
        transport.writer = writer

        node_id: Optional[str] = None

        try:
            register_message = (
                await transport.receive()
            )

            if register_message is None:
                return

            node_id = await self._handle_registration(
                register_message,
                transport,
            )

            if node_id is None:
                return

            auth_message = await transport.receive()

            if auth_message is None:
                return

            authenticated = (
                await self._handle_authentication(
                    auth_message,
                    node_id,
                    transport,
                )
            )

            if not authenticated:
                return

            self._active_nodes[node_id] = transport

            await self._handle_node_message_loop(
                node_id,
                transport,
            )

        except Exception:
            if node_id:
                try:
                    self.registry.update_node_state(
                        node_id,
                        NodeState.OFFLINE,
                    )
                except KeyError:
                    pass

        finally:
            if node_id:
                self._active_nodes.pop(
                    node_id,
                    None,
                )

            if node_id:
                self.resource_registry.remove_resources(
                    node_id
                )

            await transport.disconnect()

    async def _handle_registration(
        self,
        message: BaseMessage,
        transport: TCPTransport,
    ) -> Optional[str]:
        """Handle node registration."""

        if message.type != MessageType.REGISTER:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": "Expected REGISTER message",
                },
            )

            await transport.send(
                error_response
            )

            return None

        payload = message.payload

        if not isinstance(payload, dict):
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": "Invalid payload format",
                },
            )

            await transport.send(
                error_response
            )

            return None

        node_id = payload.get("node_id")
        hostname = payload.get("hostname")
        connection_address = payload.get(
            "connection_address"
        )
        connection_port = payload.get(
            "connection_port"
        )

        if not node_id or not hostname:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": (
                        "Missing required fields: "
                        "node_id, hostname"
                    ),
                },
            )

            await transport.send(
                error_response
            )

            return None

        try:
            self.registry.register_node(
                node_id=node_id,
                hostname=hostname,
                connection_address=(
                    connection_address
                ),
                connection_port=connection_port,
            )

            response = BaseMessage(
                type=MessageType.REGISTER_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "registered",
                    "node_id": node_id,
                },
            )

            await transport.send(response)

            return node_id

        except ValueError as exc:
            error_response = BaseMessage(
                type=MessageType.ERROR,
                message_id=str(uuid.uuid4()),
                payload={
                    "error": str(exc),
                },
            )

            await transport.send(
                error_response
            )

            return None

    async def _handle_authentication(
        self,
        message: BaseMessage,
        node_id: str,
        transport: TCPTransport,
    ) -> bool:
        """Handle node authentication."""

        if message.type != MessageType.AUTHENTICATE:
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": (
                        "Expected AUTHENTICATE message"
                    ),
                },
            )

            await transport.send(
                error_response
            )

            self.registry.update_node_state(
                node_id,
                NodeState.AUTH_FAILED,
            )

            return False

        payload = message.payload

        if not isinstance(payload, dict):
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "Invalid payload format",
                },
            )

            await transport.send(
                error_response
            )

            self.registry.update_node_state(
                node_id,
                NodeState.AUTH_FAILED,
            )

            return False

        credentials = payload.get(
            "token"
        ) or payload.get(
            "credentials"
        )

        if self.authenticator is None:
            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": (
                        "No authenticator configured"
                    ),
                },
            )

            await transport.send(
                error_response
            )

            self.registry.update_node_state(
                node_id,
                NodeState.AUTH_FAILED,
            )

            return False

        try:
            authenticated = (
                self.authenticator.authenticate(
                    credentials
                )
            )

            if authenticated:
                self.registry.authenticate_node(
                    node_id,
                    authenticated=True,
                )

                response = BaseMessage(
                    type=(
                        MessageType.AUTHENTICATE_RESPONSE
                    ),
                    message_id=str(uuid.uuid4()),
                    payload={
                        "status": "authenticated",
                        "node_id": node_id,
                    },
                )

                await transport.send(
                    response
                )

                return True

            self.registry.authenticate_node(
                node_id,
                authenticated=False,
            )

            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "Invalid credentials",
                },
            )

            await transport.send(
                error_response
            )

            return False

        except AuthenticationError as exc:
            self.registry.authenticate_node(
                node_id,
                authenticated=False,
            )

            error_response = BaseMessage(
                type=MessageType.AUTHENTICATE_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": str(exc),
                },
            )

            await transport.send(
                error_response
            )

            return False

    async def _handle_node_message_loop(
        self,
        node_id: str,
        transport: TCPTransport,
    ) -> None:
        """Receive and dispatch node messages."""

        while self._running:
            try:
                message = await transport.receive()

                if message is None:
                    break

                if message.type == MessageType.HEARTBEAT:
                    await self._handle_heartbeat(
                        node_id,
                        message,
                        transport,
                    )

                elif (
                    message.type
                    == MessageType.RESOURCE_REPORT
                ):
                    await self._handle_resource_report(
                        node_id,
                        message,
                        transport,
                    )

                elif message.type in (
                    MessageType.SERVICE_START_RESPONSE,
                    MessageType.SERVICE_STOP_RESPONSE,
                    MessageType.SERVICE_STATUS_RESPONSE,
                ):
                    self._store_service_response(
                        message,
                        node_id=node_id,
                    )

                elif (
                    message.type
                    == MessageType.SERVICE_FAILURE
                ):
                    await self._handle_service_failure(
                        node_id=node_id,
                        message=message,
                    )

                elif message.type == MessageType.ERROR:
                    self._store_service_response(
                        message,
                        node_id=node_id,
                    )

                else:
                    continue

            except asyncio.CancelledError:
                break

            except Exception:
                break

    async def _handle_heartbeat(
        self,
        node_id: str,
        message: BaseMessage,
        transport: TCPTransport,
    ) -> None:
        """Handle a heartbeat message."""

        payload = message.payload

        if not isinstance(payload, dict):
            return

        if payload.get("node_id") != node_id:
            return

        self.registry.record_heartbeat(
            node_id
        )

        response = BaseMessage(
            type=MessageType.HEARTBEAT_RESPONSE,
            message_id=message.message_id,
            payload={
                "status": "ok",
                "node_id": node_id,
            },
        )

        await transport.send(response)

    async def _handle_resource_report(
        self,
        node_id: str,
        message: BaseMessage,
        transport: TCPTransport,
    ) -> None:
        """Handle a resource report from a node."""

        payload = message.payload

        if not isinstance(payload, dict):
            response = BaseMessage(
                type=(
                    MessageType.RESOURCE_REPORT_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "node_id": node_id,
                    "request_id": message.message_id,
                    "error": (
                        "Invalid payload format"
                    ),
                },
            )

            await transport.send(response)
            return

        if payload.get("node_id") != node_id:
            response = BaseMessage(
                type=(
                    MessageType.RESOURCE_REPORT_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "node_id": node_id,
                    "request_id": message.message_id,
                    "error": "Node ID mismatch",
                },
            )

            await transport.send(response)
            return

        try:
            resources = NodeResources(
                cpu_cores=float(
                    payload["cpu_cores"]
                ),
                cpu_usage_percent=float(
                    payload[
                        "cpu_usage_percent"
                    ]
                ),
                memory_total_mb=int(
                    payload[
                        "memory_total_mb"
                    ]
                ),
                memory_used_mb=int(
                    payload[
                        "memory_used_mb"
                    ]
                ),
                disk_total_gb=float(
                    payload[
                        "disk_total_gb"
                    ]
                ),
                disk_used_gb=float(
                    payload[
                        "disk_used_gb"
                    ]
                ),
                running_services=int(
                    payload.get(
                        "running_services",
                        0,
                    )
                ),
            )

            if resources.cpu_cores < 0:
                raise ValueError(
                    "cpu_cores cannot be negative"
                )

            if not 0 <= (
                resources.cpu_usage_percent
            ) <= 100:
                raise ValueError(
                    "cpu_usage_percent must be between 0 and 100"
                )

            if resources.memory_total_mb < 0:
                raise ValueError(
                    "memory_total_mb cannot be negative"
                )

            if resources.memory_used_mb < 0:
                raise ValueError(
                    "memory_used_mb cannot be negative"
                )

            if resources.disk_total_gb < 0:
                raise ValueError(
                    "disk_total_gb cannot be negative"
                )

            if resources.disk_used_gb < 0:
                raise ValueError(
                    "disk_used_gb cannot be negative"
                )

            if resources.running_services < 0:
                raise ValueError(
                    "running_services cannot be negative"
                )

            self.resource_registry.register_resources(
                node_id,
                resources,
            )

            response = BaseMessage(
                type=(
                    MessageType.RESOURCE_REPORT_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "accepted",
                    "node_id": node_id,
                    "request_id": (
                        message.message_id
                    ),
                    "running_services": (
                        resources.running_services
                    ),
                },
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            response = BaseMessage(
                type=(
                    MessageType.RESOURCE_REPORT_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "node_id": node_id,
                    "request_id": (
                        message.message_id
                    ),
                    "error": str(exc),
                },
            )

        await transport.send(response)

    def _store_service_response(
        self,
        message: BaseMessage,
        node_id: Optional[str] = None,
    ) -> None:
        """Store response and update service state."""

        request_id = message.payload.get(
            "request_id"
        )

        if not request_id:
            request_id = message.message_id

        self._service_responses[
            request_id
        ] = message

        service_id = message.payload.get(
            "service_id"
        )

        if service_id:
            if (
                message.type
                == MessageType.SERVICE_START_RESPONSE
            ):
                resolved_node_id = (
                    node_id
                    or message.payload.get(
                        "node_id"
                    )
                )

                if resolved_node_id:
                    requirements_payload = (
                        message.payload.get(
                            "requirements",
                            {},
                        )
                    )

                    if not isinstance(
                        requirements_payload,
                        dict,
                    ):
                        requirements_payload = {}

                    requirements = (
                        ServiceRequirements.from_dict(
                            requirements_payload
                        )
                    )

                    self.service_registry.register_service(
                        service_id=service_id,
                        node_id=resolved_node_id,
                        status=message.payload.get(
                            "status",
                            "started",
                        ),
                        pid=message.payload.get(
                            "pid"
                        ),
                        command=message.payload.get(
                            "command"
                        ),
                        requirements=requirements,
                    )

            elif (
                message.type
                == MessageType.SERVICE_STATUS_RESPONSE
            ):
                existing_service = (
                    self.service_registry.get_service(
                        service_id
                    )
                )

                if existing_service is not None:
                    self.service_registry.update_service(
                        service_id=service_id,
                        status=message.payload.get(
                            "status",
                            existing_service.status,
                        ),
                        pid=message.payload.get(
                            "pid",
                            existing_service.pid,
                        ),
                    )

            elif (
                message.type
                == MessageType.SERVICE_STOP_RESPONSE
            ):
                existing_service = (
                    self.service_registry.get_service(
                        service_id
                    )
                )

                if existing_service is not None:
                    self.service_registry.update_service(
                        service_id=service_id,
                        status=message.payload.get(
                            "status",
                            "stopped",
                        ),
                        pid=None,
                    )

        event = (
            self._service_response_events.get(
                request_id
            )
        )

        if event:
            event.set()

    def _build_resource_candidates(
        self,
        exclude_node_id: Optional[str] = None,
    ) -> list[ResourceNodeCandidate]:
        """Build candidates from authenticated online nodes."""

        candidates = []

        for node in self.registry.list_nodes():
            if node.state != NodeState.ONLINE:
                continue

            if not node.authenticated:
                continue

            if node.node_id == exclude_node_id:
                continue

            resources = (
                self.resource_registry.get_resources(
                    node.node_id
                )
            )

            if resources is None:
                continue

            candidates.append(
                ResourceNodeCandidate(
                    node_id=node.node_id,
                    available=(
                        node.node_id
                        in self._active_nodes
                    ),
                    running_services=len(
                        self.service_registry.list_node_services(
                            node.node_id
                        )
                    ),
                    resources=resources,
                )
            )

        return candidates

    def select_node_for_service(
        self,
        required_cpu_cores: float = 0.0,
        required_memory_mb: int = 0,
        required_disk_gb: float = 0.0,
        exclude_node_id: Optional[str] = None,
    ) -> Optional[ResourceNodeCandidate]:
        """Select a node with enough resources."""

        requirements = ServiceRequirements(
            cpu_cores=required_cpu_cores,
            memory_mb=required_memory_mb,
            disk_gb=required_disk_gb,
        )

        candidates = self._build_resource_candidates(
            exclude_node_id=exclude_node_id,
        )

        return (
            self.resource_scheduler.select_node_for_requirements(
                nodes=candidates,
                requirements=requirements,
            )
        )

    async def start_service_auto(
        self,
        service_id: str,
        command: str,
        required_cpu_cores: float = 0.0,
        required_memory_mb: int = 0,
        required_disk_gb: float = 0.0,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Select a suitable node and start a service."""

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        if not command:
            raise ValueError(
                "command is required"
            )

        requirements = ServiceRequirements(
            cpu_cores=required_cpu_cores,
            memory_mb=required_memory_mb,
            disk_gb=required_disk_gb,
        )

        candidates = self._build_resource_candidates()

        placement = self.service_placement.select_node(
            service_id=service_id,
            requirements=requirements,
            nodes=candidates,
        )

        if placement is None:
            raise RuntimeError(
                "No available node has enough resources"
            )

        return await self.start_service(
            node_id=placement.node_id,
            service_id=service_id,
            command=command,
            requirements=requirements,
            timeout_seconds=timeout_seconds,
        )

    async def start_service(
        self,
        node_id: str,
        service_id: str,
        command: str,
        requirements: Optional[
            ServiceRequirements
        ] = None,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Request a node to start a service."""

        transport = self._active_nodes.get(
            node_id
        )

        if transport is None:
            raise RuntimeError(
                f"Node {node_id} is not connected"
            )

        if not service_id:
            raise ValueError(
                "service_id is required"
            )

        if not command:
            raise ValueError(
                "command is required"
            )

        if requirements is None:
            requirements = ServiceRequirements()

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            raise TypeError(
                "requirements must be a ServiceRequirements instance"
            )

        request_id = str(uuid.uuid4())

        event = asyncio.Event()

        self._service_response_events[
            request_id
        ] = event

        message = BaseMessage(
            type=MessageType.SERVICE_START,
            message_id=request_id,
            payload={
                "service_id": service_id,
                "command": command,
                "requirements": (
                    requirements.to_dict()
                ),
                "request_id": request_id,
            },
        )

        try:
            await transport.send(message)

            await asyncio.wait_for(
                event.wait(),
                timeout=timeout_seconds,
            )

            response = (
                self._service_responses.get(
                    request_id
                )
            )

            if response is None:
                raise RuntimeError(
                    "Service start response was not received"
                )

            if (
                response.payload.get(
                    "status"
                )
                == "started"
            ):
                self.service_registry.register_service(
                    service_id=service_id,
                    node_id=node_id,
                    status="running",
                    pid=response.payload.get(
                        "pid"
                    ),
                    command=command,
                    requirements=requirements,
                )

                existing_reservation = (
                    self.resource_accounting.get(
                        service_id
                    )
                )

                if existing_reservation is None:
                    self.migration_manager.reserve_service(
                        service_id=service_id,
                        node_id=node_id,
                        requirements=requirements,
                    )
                elif (
                    existing_reservation.node_id
                    != node_id
                ):
                    self.migration_manager.migrate_reservation(
                        service_id=service_id,
                        target_node_id=node_id,
                    )

            return response

        finally:
            self._service_response_events.pop(
                request_id,
                None,
            )

            self._service_responses.pop(
                request_id,
                None,
            )

    async def migrate_service(
        self,
        service_id: str,
        failed_node_id: str,
        timeout_seconds: float = 10.0,
    ) -> Optional[BaseMessage]:
        """Perform a complete resource-aware migration."""

        service = (
            self.service_registry.get_service(
                service_id
            )
        )

        if service is None:
            return None

        if service.node_id != failed_node_id:
            return None

        if self.migration_manager.is_migrating(
            service_id
        ):
            return None

        command = service.command

        if not command:
            return None

        requirements = getattr(
            service,
            "requirements",
            ServiceRequirements(),
        )

        if not isinstance(
            requirements,
            ServiceRequirements,
        ):
            requirements = ServiceRequirements()

        candidates = self._build_resource_candidates(
            exclude_node_id=failed_node_id,
        )

        plan = (
            self.resource_failover.create_migration_plan(
                service_id=service_id,
                source_node_id=failed_node_id,
                command=command,
                requirements=requirements,
                nodes=candidates,
            )
        )

        if plan is None:
            return None

        self.migration_registry.start(
            service_id=plan.service_id,
            source_node_id=plan.source_node_id,
            target_node_id=plan.target_node_id,
        )

        async def start_target(
            node_id,
            service_id,
            command,
            requirements,
        ):
            return await self.start_service(
                node_id=node_id,
                service_id=service_id,
                command=command,
                requirements=requirements,
                timeout_seconds=timeout_seconds,
            )

        async def verify_target(
            node_id,
            service_id,
        ):
            try:
                response = await self.status_service(
                    node_id=node_id,
                    service_id=service_id,
                    timeout_seconds=timeout_seconds,
                )

                return (
                    response.payload.get(
                        "status"
                    )
                    in {
                        "running",
                        "started",
                    }
                )

            except (
                RuntimeError,
                TimeoutError,
                KeyError,
            ):
                return False

        result = await self.migration_manager.execute(
            plan=plan,
            start_service=start_target,
            verify_service=verify_target,
        )

        if result.status == "migrated":
            self.service_registry.move_service(
                service_id=service_id,
                node_id=plan.target_node_id,
                pid=result.pid,
                status="running",
            )

            self.service_registry.update_service(
                service_id=service_id,
                command=plan.command,
                requirements=plan.requirements,
            )

            self.migration_registry.complete(
                service_id=service_id,
                pid=result.pid,
            )

            self.failure_manager.clear_failure(
                service_id
            )

            return BaseMessage(
                type=(
                    MessageType.SERVICE_START_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "migrated",
                    "service_id": service_id,
                    "source_node_id": (
                        failed_node_id
                    ),
                    "target_node_id": (
                        plan.target_node_id
                    ),
                    "pid": result.pid,
                    "command": plan.command,
                    "requirements": (
                        plan.requirements.to_dict()
                    ),
                },
            )

        self.migration_registry.fail(
            service_id=service_id,
            error=(
                result.error
                or "Migration failed"
            ),
            status=result.status,
        )

        return BaseMessage(
            type=(
                MessageType.SERVICE_START_RESPONSE
            ),
            message_id=str(uuid.uuid4()),
            payload={
                "status": result.status,
                "service_id": service_id,
                "source_node_id": (
                    failed_node_id
                ),
                "target_node_id": (
                    plan.target_node_id
                ),
                "error": result.error,
            },
        )

    async def stop_service(
        self,
        node_id: str,
        service_id: str,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Request a node to stop a service."""

        transport = self._active_nodes.get(
            node_id
        )

        if transport is None:
            raise RuntimeError(
                f"Node {node_id} is not connected"
            )

        request_id = str(uuid.uuid4())

        event = asyncio.Event()

        self._service_response_events[
            request_id
        ] = event

        message = BaseMessage(
            type=MessageType.SERVICE_STOP,
            message_id=request_id,
            payload={
                "service_id": service_id,
                "request_id": request_id,
            },
        )

        try:
            await transport.send(message)

            await asyncio.wait_for(
                event.wait(),
                timeout=timeout_seconds,
            )

            response = (
                self._service_responses.get(
                    request_id
                )
            )

            if response is None:
                raise RuntimeError(
                    "Service stop response was not received"
                )

            if response.payload.get(
                "status"
            ) in {
                "stopped",
                "success",
            }:
                self.resource_accounting.release(
                    service_id
                )

                self.service_registry.update_service(
                    service_id=service_id,
                    status=response.payload.get(
                        "status",
                        "stopped",
                    ),
                    pid=None,
                )

            return response

        finally:
            self._service_response_events.pop(
                request_id,
                None,
            )

            self._service_responses.pop(
                request_id,
                None,
            )

    async def status_service(
        self,
        node_id: str,
        service_id: str,
        timeout_seconds: float = 10.0,
    ) -> BaseMessage:
        """Request the current service status."""

        transport = self._active_nodes.get(
            node_id
        )

        if transport is None:
            raise RuntimeError(
                f"Node {node_id} is not connected"
            )

        request_id = str(uuid.uuid4())

        event = asyncio.Event()

        self._service_response_events[
            request_id
        ] = event

        message = BaseMessage(
            type=MessageType.SERVICE_STATUS,
            message_id=request_id,
            payload={
                "service_id": service_id,
                "request_id": request_id,
            },
        )

        try:
            await transport.send(message)

            await asyncio.wait_for(
                event.wait(),
                timeout=timeout_seconds,
            )

            response = (
                self._service_responses.get(
                    request_id
                )
            )

            if response is None:
                raise RuntimeError(
                    "Service status response was not received"
                )

            return response

        finally:
            self._service_response_events.pop(
                request_id,
                None,
            )

            self._service_responses.pop(
                request_id,
                None,
            )

    async def _handle_service_failure(
        self,
        node_id: str,
        message: BaseMessage,
    ) -> None:
        """Handle a terminal service failure."""

        payload = message.payload

        if not isinstance(payload, dict):
            return

        service_id = payload.get(
            "service_id"
        )

        if not service_id:
            return

        status = payload.get(
            "status",
            "crashed",
        )

        reason = payload.get(
            "error"
        )

        restart_attempts = payload.get(
            "restart_attempts",
            0,
        )

        self.failure_manager.record_failure(
            service_id=service_id,
            node_id=node_id,
            status=status,
            reason=reason,
            restart_attempts=restart_attempts,
        )

        service = (
            self.service_registry.get_service(
                service_id
            )
        )

        if service is None:
            return

        self.service_registry.update_service(
            service_id=service_id,
            status=status,
        )

        try:
            await self.migrate_service(
                service_id=service.service_id,
                failed_node_id=node_id,
            )

        except Exception:
            return

    async def _run_service_health_monitor(
        self,
    ) -> None:
        """Monitor registered services."""

        while self._running:
            try:
                await asyncio.sleep(
                    self.service_health_interval_seconds
                )

                if not self._running:
                    break

                services = (
                    self.service_registry.list_services()
                )

                for service in services:
                    try:
                        if service.status in {
                            "stopped",
                            "failed",
                        }:
                            continue

                        await self.status_service(
                            node_id=service.node_id,
                            service_id=service.service_id,
                        )

                    except (
                        RuntimeError,
                        TimeoutError,
                        KeyError,
                    ):
                        continue

            except asyncio.CancelledError:
                break

            except Exception:
                continue

    async def _run_offline_detection(
        self,
    ) -> None:
        """Run offline node detection."""

        while self._running:
            try:
                await asyncio.sleep(
                    self.heartbeat_timeout_seconds
                    / 2
                )

                if not self._running:
                    break

                offline_nodes = (
                    self.registry.detect_offline_nodes(
                        self.heartbeat_timeout_seconds
                    )
                )

                for node_info in offline_nodes:
                    self.registry.mark_offline(
                        node_info.node_id
                    )

                    self.resource_registry.remove_resources(
                        node_info.node_id
                    )

            except asyncio.CancelledError:
                break

            except Exception:
                pass