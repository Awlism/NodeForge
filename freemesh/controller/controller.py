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
from freemesh.controller.reconciler import (
    Reconciler,
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
from freemesh.controller.service_intent import (
    DesiredState,
    ServiceIntent,
)
from freemesh.controller.service_intent_registry import (
    ServiceIntentRegistry,
)
from freemesh.controller.service_intent_store import (
    ServiceIntentStore,
)
from freemesh.controller.service_metadata_store import (
    ServiceMetadataStore,
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
        reconciliation_interval_seconds: float = 5.0,
        database_path: str = ":memory:",
    ):
        self.host = host
        self.port = port
        self.heartbeat_timeout_seconds = (
            heartbeat_timeout_seconds
        )
        self.authenticator = authenticator
        self.reconciliation_interval_seconds = (
            reconciliation_interval_seconds
        )

        self.registry = NodeRegistry(
            database_path=database_path,
        )

        self.resource_registry = ResourceRegistry(
            database_path=database_path,
        )

        self.service_metadata_store = (
            ServiceMetadataStore(database_path)
        )

        self.service_registry = ServiceRegistry(
            metadata_store=self.service_metadata_store,
        )

        self.service_intent_store = (
            ServiceIntentStore(database_path)
        )

        self.service_intent_registry = (
            ServiceIntentRegistry(
                store=self.service_intent_store,
            )
        )

        self.service_intent_registry.load_from_store()

        self.reconciler = Reconciler(
            self.service_intent_registry
        )

        self.failure_manager = FailureManager()

        self.failover_manager = FailoverManager()

        # Resource accounting must exist before the scheduler.
        self.resource_accounting = ResourceAccounting()

        self.resource_scheduler = ResourceScheduler(
            accounting=self.resource_accounting,
        )

        self.service_placement = ServicePlacement(
            scheduler=self.resource_scheduler,
        )

        self.resource_failover = ResourceFailover(
            scheduler=self.resource_scheduler,
        )

        self.migration_manager = MigrationManager(
            accounting=self.resource_accounting,
        )

        self.migration_registry = MigrationRegistry()

        # Only one migration transaction may modify resource
        # accounting at a time.
        #
        # Without this lock two simultaneous failures can both
        # select the same target before either reservation has
        # been moved, which causes capacity races.
        self._migration_lock = asyncio.Lock()

        self.server: Optional[asyncio.Server] = None
        self._running = False

        self._offline_detection_task: Optional[
            asyncio.Task
        ] = None

        self._service_health_task: Optional[
            asyncio.Task
        ] = None

        self._reconciliation_task: Optional[
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

        # Request IDs whose response must NOT synchronize the
        # canonical ServiceRegistry.
        #
        # Used mainly during migration transactions:
        #
        # 1. target is started
        # 2. target is verified
        # 3. source is fenced
        # 4. migration commits
        #
        # The registry should move only at step 4.
        self._suppress_service_state_sync: set[
            str
        ] = set()

    # =========================================================
    # SERVICE INTENT
    # =========================================================

    def create_service_intent(
        self,
        service_id: str,
        command: str,
        requirements: ServiceRequirements | None = None,
        desired_state: DesiredState = DesiredState.RUNNING,
    ) -> ServiceIntent:
        """Create and register a desired service intent."""

        if requirements is None:
            requirements = ServiceRequirements()

        intent = ServiceIntent(
            service_id=service_id,
            desired_state=desired_state,
            command=command,
            requirements=requirements,
        )

        return self.service_intent_registry.register(
            intent
        )

    def get_service_intent(
        self,
        service_id: str,
    ) -> ServiceIntent | None:
        """Return the desired intent for a service."""

        return self.service_intent_registry.get(
            service_id
        )

    def set_service_desired_state(
        self,
        service_id: str,
        desired_state: DesiredState,
    ) -> ServiceIntent:
        """Change the desired state of a service."""

        return self.service_intent_registry.set_desired_state(
            service_id,
            desired_state,
        )

    def remove_service_intent(
        self,
        service_id: str,
    ) -> ServiceIntent:
        """Remove a service desired intent."""

        return self.service_intent_registry.remove(
            service_id
        )

    # =========================================================
    # RECONCILIATION
    # =========================================================

    async def reconcile_service(
        self,
        service_id: str,
    ):
        """Reconcile one service with its desired state."""

        actual_service = (
            self.service_registry.get_service(
                service_id
            )
        )

        async def start_service_for_reconcile(
            service_id: str,
            command: str,
            requirements: ServiceRequirements,
        ):
            """Start a service using automatic placement."""

            return await self.start_service_auto(
                service_id=service_id,
                command=command,
                required_cpu_cores=(
                    requirements.cpu_cores
                ),
                required_memory_mb=(
                    requirements.memory_mb
                ),
                required_disk_gb=(
                    requirements.disk_gb
                ),
            )

        async def stop_service_for_reconcile(
            service_id: str,
        ):
            """Stop a service on its currently assigned node."""

            service = (
                self.service_registry.get_service(
                    service_id
                )
            )

            if service is None:
                return None

            return await self.stop_service(
                node_id=service.node_id,
                service_id=service_id,
            )

        async def migrate_service_for_reconcile(
            service_id: str,
        ):
            """Migrate a service away from its current node."""

            service = (
                self.service_registry.get_service(
                    service_id
                )
            )

            if service is None:
                return None

            return await self.migrate_service(
                service_id=service_id,
                failed_node_id=service.node_id,
            )

        return await self.reconciler.reconcile(
            service_id=service_id,
            actual_service=actual_service,
            start_service=(
                start_service_for_reconcile
            ),
            stop_service=(
                stop_service_for_reconcile
            ),
            migrate_service=(
                migrate_service_for_reconcile
            ),
        )

    async def reconcile_all_services(self):
        """Reconcile all registered service intents."""

        results = []

        for intent in (
            self.service_intent_registry.list_all()
        ):
            try:
                result = await self.reconcile_service(
                    intent.service_id
                )

                results.append(result)

            except Exception as exc:
                from freemesh.controller.reconciler import (
                    ReconciliationResult,
                )

                results.append(
                    ReconciliationResult(
                        service_id=(
                            intent.service_id
                        ),
                        action="error",
                        changed=False,
                        reason=str(exc),
                    )
                )

        return results

    # =========================================================
    # CONTROLLER PERSISTENCE
    # =========================================================

    def close(self) -> None:
        """Close persistent controller resources."""

        self.service_intent_store.close()
        self.service_metadata_store.close()
        self.resource_registry.close()
        self.registry.close()

    # =========================================================
    # CONTROLLER LIFECYCLE
    # =========================================================

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

        self._reconciliation_task = (
            asyncio.create_task(
                self._run_background_reconciliation()
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

        if self._reconciliation_task:
            self._reconciliation_task.cancel()

            try:
                await self._reconciliation_task
            except asyncio.CancelledError:
                pass

            self._reconciliation_task = None

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
        self._suppress_service_state_sync.clear()

        self.resource_accounting.clear()

    # =========================================================
    # CONNECTION HANDLING
    # =========================================================

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

        except asyncio.CancelledError:
            raise

        except Exception:
            pass

        finally:
            if node_id:
                self._active_nodes.pop(
                    node_id,
                    None,
                )

                try:
                    node_info = (
                        self.registry.get_node(
                            node_id
                        )
                    )
                except KeyError:
                    node_info = None

                if node_info is not None:
                    if (
                        node_info.state
                        == NodeState.ONLINE
                    ):
                        try:
                            self.registry.mark_offline(
                                node_id
                            )
                        except KeyError:
                            pass

                        self.resource_registry.remove_resources(
                            node_id
                        )

                        # A controller shutdown must never be
                        # interpreted as a node failure.
                        if self._running:
                            try:
                                await self._recover_services_from_node(
                                    node_id
                                )
                            except Exception:
                                pass

                    else:
                        self.resource_registry.remove_resources(
                            node_id
                        )

                else:
                    self.resource_registry.remove_resources(
                        node_id
                    )

            try:
                await transport.disconnect()
            except Exception:
                pass

    # =========================================================
    # REGISTRATION / AUTHENTICATION
    # =========================================================

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

    # =========================================================
    # NODE MESSAGE LOOP
    # =========================================================

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

    # =========================================================
    # HEARTBEAT / RESOURCES
    # =========================================================

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

    # =========================================================
    # SERVICE RESPONSE / REGISTRY
    # =========================================================

    def _store_service_response(
        self,
        message: BaseMessage,
        node_id: Optional[str] = None,
    ) -> None:
        """Store a service response and synchronize state."""

        payload = message.payload

        if not isinstance(payload, dict):
            return

        request_id = payload.get(
            "request_id"
        )

        if not request_id:
            request_id = message.message_id

        self._service_responses[
            request_id
        ] = message

        service_id = payload.get(
            "service_id"
        )

        sync_registry = (
            request_id
            not in self._suppress_service_state_sync
        )

        if service_id and sync_registry:
            if (
                message.type
                == MessageType.SERVICE_START_RESPONSE
            ):
                resolved_node_id = (
                    node_id
                    or payload.get("node_id")
                )

                if resolved_node_id:
                    requirements_payload = (
                        payload.get(
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
                        status=payload.get(
                            "status",
                            "started",
                        ),
                        pid=payload.get(
                            "pid"
                        ),
                        command=payload.get(
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
                    runtime_status = payload.get(
                        "status",
                        existing_service.status,
                    )

                    original_runtime_status = (
                        runtime_status
                    )

                    # A response from an old/non-owner node
                    # must never overwrite canonical state when
                    # it says the service is absent or stopped.
                    #
                    # This is particularly important after
                    # migration: the old node may still answer a
                    # delayed STATUS request with "not_found".
                    is_current_owner = (
                        node_id is not None
                        and existing_service.node_id
                        == node_id
                    )

                    if (
                        not is_current_owner
                        and original_runtime_status
                        in {
                            "not_found",
                            "stopped",
                        }
                    ):
                        runtime_status = None

                    if runtime_status is not None:
                        if runtime_status == "not_found":
                            runtime_status = "stopped"

                        update_kwargs = {
                            "service_id": service_id,
                            "status": runtime_status,
                        }

                        # Ownership may only move when a positive
                        # runtime state proves that this node is
                        # actually running the service.
                        if (
                            payload.get("node_id")
                            and original_runtime_status
                            not in {
                                "not_found",
                                "stopped",
                            }
                        ):
                            update_kwargs["node_id"] = (
                                payload["node_id"]
                            )

                        if payload.get("command"):
                            update_kwargs["command"] = (
                                payload["command"]
                            )

                        if payload.get(
                            "requirements"
                        ) is not None:
                            try:
                                update_kwargs[
                                    "requirements"
                                ] = (
                                    ServiceRequirements.from_dict(
                                        payload[
                                            "requirements"
                                        ]
                                    )
                                )
                            except (
                                TypeError,
                                ValueError,
                            ):
                                pass

                        if runtime_status == "stopped":
                            update_kwargs["pid"] = None
                        elif payload.get("pid") is not None:
                            update_kwargs["pid"] = (
                                payload["pid"]
                            )

                        self.service_registry.update_service(
                            **update_kwargs
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

                # A delayed stop response from a previous owner
                # must not stop a service that has already moved.
                if (
                    existing_service is not None
                    and (
                        node_id is None
                        or existing_service.node_id
                        == node_id
                    )
                ):
                    self.service_registry.update_service(
                        service_id=service_id,
                        status=payload.get(
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

    # =========================================================
    # RESOURCE PLACEMENT
    # =========================================================

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

    # =========================================================
    # SERVICE START
    # =========================================================

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
        reserve_resources: bool = True,
        update_registry: bool = True,
    ) -> BaseMessage:
        """Request a node to start a service.

        ``reserve_resources`` controls ResourceAccounting.

        ``update_registry`` controls ServiceRegistry.

        Migration uses both as transactional controls.
        """

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

        if not update_registry:
            self._suppress_service_state_sync.add(
                request_id
            )

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

            response_status = (
                response.payload.get(
                    "status"
                )
                if isinstance(
                    response.payload,
                    dict,
                )
                else None
            )

            if response_status in {
                "started",
                "running",
            }:
                if update_registry:
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

                if reserve_resources:
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

            self._suppress_service_state_sync.discard(
                request_id
            )

    # =========================================================
    # SERVICE MIGRATION
    # =========================================================

    async def migrate_service(
        self,
        service_id: str,
        failed_node_id: str,
        timeout_seconds: float = 10.0,
    ) -> Optional[BaseMessage]:
        """Perform one serialized resource-aware migration."""

        async with self._migration_lock:
            return await self._migrate_service_unlocked(
                service_id=service_id,
                failed_node_id=failed_node_id,
                timeout_seconds=timeout_seconds,
            )

    async def _migrate_service_unlocked(
        self,
        service_id: str,
        failed_node_id: str,
        timeout_seconds: float = 10.0,
    ) -> Optional[BaseMessage]:
        """Execute a migration while holding the migration lock."""

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

        original_node_id = service.node_id
        original_pid = service.pid
        original_status = service.status
        original_command = service.command
        original_requirements = (
            service.requirements
        )

        original_reservation = (
            self.resource_accounting.get(
                service_id
            )
        )

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
            return BaseMessage(
                type=(
                    MessageType.SERVICE_START_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "capacity_unavailable",
                    "service_id": service_id,
                    "source_node_id": (
                        failed_node_id
                    ),
                    "target_node_id": None,
                    "error": (
                        "No available node has enough "
                        "resources for migration"
                    ),
                },
            )

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
            """Start target without registry/reservation commit."""

            return await self.start_service(
                node_id=node_id,
                service_id=service_id,
                command=command,
                requirements=requirements,
                timeout_seconds=timeout_seconds,
                reserve_resources=False,
                update_registry=False,
            )

        async def verify_target(
            node_id,
            service_id,
        ):
            """Verify target runtime without registry mutation."""

            try:
                response = await self.status_service(
                    node_id=node_id,
                    service_id=service_id,
                    timeout_seconds=timeout_seconds,
                    update_registry=False,
                )

                payload = response.payload

                if not isinstance(
                    payload,
                    dict,
                ):
                    return False

                return payload.get(
                    "status"
                ) in {
                    "running",
                    "started",
                }

            except Exception:
                # Verification is observational. Any runtime,
                # transport, timeout, or test/double incompatibility
                # must be treated as a failed verification so
                # MigrationManager can perform its normal
                # transactional rollback and report
                # "verification_failed" instead of converting the
                # problem into a generic migration failure.
                return False

        async def stop_target(
            node_id,
            service_id,
        ):
            """Stop target runtime during rollback."""

            return await self.stop_service(
                node_id=node_id,
                service_id=service_id,
                timeout_seconds=timeout_seconds,
                release_resources=False,
                update_registry=False,
            )

        async def stop_source_before_commit(
            node_id,
            service_id,
        ):
            """Fence source runtime before migration commit."""

            # If the source node is already disconnected, the
            # Controller has already fenced it from the
            # distributed system perspective.
            if node_id not in self._active_nodes:
                return BaseMessage(
                    type=(
                        MessageType.SERVICE_STOP_RESPONSE
                    ),
                    message_id=str(uuid.uuid4()),
                    payload={
                        "status": "stopped",
                        "service_id": service_id,
                        "node_id": node_id,
                        "request_id": str(uuid.uuid4()),
                    },
                )

            response = await self.stop_service(
                node_id=node_id,
                service_id=service_id,
                timeout_seconds=timeout_seconds,
                release_resources=False,
                update_registry=False,
            )

            payload = response.payload

            if not isinstance(
                payload,
                dict,
            ):
                raise RuntimeError(
                    "Invalid source stop response"
                )

            if payload.get(
                "status"
            ) not in {
                "stopped",
                "success",
                "not_found",
            }:
                raise RuntimeError(
                    "Source service could not be stopped "
                    "before migration commit"
                )

            return response

        try:
            result = await self.migration_manager.execute(
                plan=plan,
                start_service=start_target,
                verify_service=verify_target,
                stop_service=stop_target,
                pre_commit=stop_source_before_commit,
            )

        except Exception as exc:
            # The MigrationManager normally converts transaction
            # failures into MigrationResult. This block is only
            # for unexpected Controller-side exceptions.
            self.service_registry.update_service(
                service_id=service_id,
                node_id=original_node_id,
                status=original_status,
                pid=original_pid,
                command=original_command,
                requirements=original_requirements,
            )

            self.migration_registry.fail(
                service_id=service_id,
                error=str(exc),
                status="failed",
            )

            return BaseMessage(
                type=(
                    MessageType.SERVICE_START_RESPONSE
                ),
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "service_id": service_id,
                    "source_node_id": (
                        failed_node_id
                    ),
                    "target_node_id": (
                        plan.target_node_id
                    ),
                    "error": str(exc),
                },
            )

        if result.status == "migrated":
            # MigrationManager has successfully completed its
            # reservation transaction.
            #
            # Only now do we move canonical service ownership.
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

        # =====================================================
        # MIGRATION ROLLBACK
        # =====================================================

        self.service_registry.update_service(
            service_id=service_id,
            node_id=original_node_id,
            status=original_status,
            pid=original_pid,
            command=original_command,
            requirements=original_requirements,
        )

        # Restore the original reservation only when necessary.
        #
        # If the source was already offline, the recovery path
        # releases the dead-node reservation before migration.
        # In that case original_reservation is None and we must
        # not recreate a reservation on the failed node.
        if original_reservation is not None:
            current_reservation = (
                self.resource_accounting.get(
                    service_id
                )
            )

            if current_reservation is None:
                self.resource_accounting.reserve(
                    service_id=service_id,
                    node_id=(
                        original_reservation.node_id
                    ),
                    cpu_cores=(
                        original_reservation.cpu_cores
                    ),
                    memory_mb=(
                        original_reservation.memory_mb
                    ),
                    disk_gb=(
                        original_reservation.disk_gb
                    ),
                )

            elif (
                current_reservation.node_id
                != original_reservation.node_id
            ):
                self.resource_accounting.release(
                    service_id
                )

                self.resource_accounting.reserve(
                    service_id=service_id,
                    node_id=(
                        original_reservation.node_id
                    ),
                    cpu_cores=(
                        original_reservation.cpu_cores
                    ),
                    memory_mb=(
                        original_reservation.memory_mb
                    ),
                    disk_gb=(
                        original_reservation.disk_gb
                    ),
                )

        # Preserve the exact transaction result.
        #
        # In particular, verification failure must remain
        # "verification_failed" rather than being collapsed into
        # the generic "failed" state.
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

    # =========================================================
    # SERVICE STOP / STATUS
    # =========================================================

    async def stop_service(
        self,
        node_id: str,
        service_id: str,
        timeout_seconds: float = 10.0,
        release_resources: bool = True,
        update_registry: bool = True,
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

        if not update_registry:
            self._suppress_service_state_sync.add(
                request_id
            )

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

            payload = response.payload

            response_status = (
                payload.get("status")
                if isinstance(
                    payload,
                    dict,
                )
                else None
            )

            if response_status in {
                "stopped",
                "success",
                "not_found",
            }:
                if release_resources:
                    self.resource_accounting.release(
                        service_id
                    )

                if update_registry:
                    service = (
                        self.service_registry.get_service(
                            service_id
                        )
                    )

                    # Do not allow a delayed response from a
                    # previous owner to stop a service that has
                    # already migrated.
                    if (
                        service is not None
                        and service.node_id == node_id
                    ):
                        self.service_registry.update_service(
                            service_id=service_id,
                            status="stopped",
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

            self._suppress_service_state_sync.discard(
                request_id
            )

    async def status_service(
        self,
        node_id: str,
        service_id: str,
        timeout_seconds: float = 10.0,
        update_registry: bool = True,
    ) -> BaseMessage:
        """Request service status.

        ``update_registry=False`` is used by migration verification
        and failure detection when the runtime response must not
        alter canonical ownership.
        """

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

        if not update_registry:
            self._suppress_service_state_sync.add(
                request_id
            )

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

            if update_registry:
                payload = response.payload

                if isinstance(
                    payload,
                    dict,
                ):
                    status = payload.get(
                        "status"
                    )

                    service = (
                        self.service_registry.get_service(
                            service_id
                        )
                    )

                    if service is not None and status:
                        original_runtime_status = status

                        # Canonical ownership is the authoritative
                        # source for deciding whether a runtime
                        # observation may change ServiceRegistry.
                        is_current_owner = (
                            node_id == service.node_id
                        )

                        # A stale response from a previous owner
                        # saying "not_found" or "stopped" must not
                        # destroy the canonical state of a service
                        # that has already migrated.
                        if (
                            not is_current_owner
                            and original_runtime_status
                            in {
                                "not_found",
                                "stopped",
                            }
                        ):
                            return response

                        runtime_status = status

                        if runtime_status == "not_found":
                            runtime_status = "stopped"

                        update_kwargs = {
                            "service_id": service_id,
                            "status": runtime_status,
                        }

                        # Only positive runtime states are allowed
                        # to establish ownership.
                        if (
                            payload.get("node_id")
                            and original_runtime_status
                            not in {
                                "not_found",
                                "stopped",
                            }
                        ):
                            update_kwargs["node_id"] = (
                                payload["node_id"]
                            )

                        if payload.get("command"):
                            update_kwargs["command"] = (
                                payload["command"]
                            )

                        if payload.get(
                            "requirements"
                        ) is not None:
                            try:
                                update_kwargs[
                                    "requirements"
                                ] = (
                                    ServiceRequirements.from_dict(
                                        payload[
                                            "requirements"
                                        ]
                                    )
                                )
                            except (
                                TypeError,
                                ValueError,
                            ):
                                pass

                        if runtime_status == "stopped":
                            update_kwargs["pid"] = None

                        elif payload.get("pid") is not None:
                            update_kwargs["pid"] = (
                                payload["pid"]
                            )

                        self.service_registry.update_service(
                            **update_kwargs
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

            self._suppress_service_state_sync.discard(
                request_id
            )

    # =========================================================
    # FAILURE / RECOVERY
    # =========================================================

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

        service = (
            self.service_registry.get_service(
                service_id
            )
        )

        if service is None:
            return

        # Ignore stale failure reports from an old node after
        # the service has already migrated elsewhere.
        if service.node_id != node_id:
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

        self.service_registry.update_service(
            service_id=service_id,
            status=status,
        )

        try:
            await self.migrate_service(
                service_id=service.service_id,
                failed_node_id=node_id,
            )

        except (
            RuntimeError,
            TimeoutError,
            KeyError,
            ValueError,
        ):
            return

        except Exception:
            return

    async def _recover_services_from_node(
        self,
        node_id: str,
    ) -> None:
        """Automatically recover services from an offline node."""

        services = (
            self.service_registry.list_node_services(
                node_id
            )
        )

        for service in services:
            try:
                if service.status in {
                    "stopped",
                    "failed",
                }:
                    continue

                # The node is already offline, so its resource
                # reservation cannot remain attached to it while
                # failover is being planned.
                self.resource_accounting.release(
                    service.service_id
                )

                await self.migrate_service(
                    service_id=service.service_id,
                    failed_node_id=node_id,
                )

            except (
                RuntimeError,
                TimeoutError,
                KeyError,
                ValueError,
            ):
                continue

            except Exception:
                continue

    # =========================================================
    # BACKGROUND MONITORS
    # =========================================================

    async def _run_background_reconciliation(
        self,
    ) -> None:
        """Continuously reconcile desired and actual state."""

        while self._running:
            try:
                await asyncio.sleep(
                    self.reconciliation_interval_seconds
                )

                if not self._running:
                    break

                await self.reconcile_all_services()

            except asyncio.CancelledError:
                break

            except Exception:
                continue

    async def _run_service_health_monitor(
        self,
    ) -> None:
        """Monitor registered services and trigger recovery."""

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

                        node_transport = (
                            self._active_nodes.get(
                                service.node_id
                            )
                        )

                        # If the node itself is gone, this is a
                        # node failure and migration is appropriate.
                        if node_transport is None:
                            await self.migrate_service(
                                service_id=(
                                    service.service_id
                                ),
                                failed_node_id=(
                                    service.node_id
                                ),
                            )

                            continue

                        # Health checks are observational.
                        # They do not directly mutate canonical
                        # ownership.
                        response = (
                            await self.status_service(
                                node_id=service.node_id,
                                service_id=(
                                    service.service_id
                                ),
                                update_registry=False,
                            )
                        )

                        payload = response.payload

                        if not isinstance(
                            payload,
                            dict,
                        ):
                            continue

                        runtime_status = payload.get(
                            "status"
                        )

                        # -------------------------------------------------
                        # Runtime divergence:
                        #
                        # The node is healthy, but the process is missing
                        # or stopped.
                        #
                        # This is NOT automatically a node failure.
                        #
                        # We synchronize canonical state to "stopped".
                        # The desired-state reconciler then sees:
                        #
                        #     desired = RUNNING
                        #     actual  = STOPPED
                        #
                        # and starts the service again.
                        # -------------------------------------------------
                        if runtime_status in {
                            "not_found",
                            "stopped",
                        }:
                            current_service = (
                                self.service_registry.get_service(
                                    service.service_id
                                )
                            )

                            if (
                                current_service is not None
                                and current_service.node_id
                                == service.node_id
                            ):
                                self.service_registry.update_service(
                                    service_id=(
                                        service.service_id
                                    ),
                                    status="stopped",
                                    pid=None,
                                )

                            continue

                        # -------------------------------------------------
                        # Terminal runtime failure:
                        #
                        # A crash/failure on an otherwise healthy node
                        # is handled as service failure and can trigger
                        # migration.
                        # -------------------------------------------------
                        if runtime_status in {
                            "crashed",
                            "failed",
                        }:
                            failure_message = BaseMessage(
                                type=(
                                    MessageType.SERVICE_FAILURE
                                ),
                                message_id=str(
                                    uuid.uuid4()
                                ),
                                payload={
                                    "service_id": (
                                        service.service_id
                                    ),
                                    "node_id": (
                                        service.node_id
                                    ),
                                    "status": (
                                        runtime_status
                                    ),
                                    "error": (
                                        "Health monitor "
                                        "detected runtime "
                                        f"status: "
                                        f"{runtime_status}"
                                    ),
                                    "restart_attempts": 0,
                                },
                            )

                            await self._handle_service_failure(
                                node_id=service.node_id,
                                message=failure_message,
                            )

                    except (
                        RuntimeError,
                        TimeoutError,
                        KeyError,
                        ValueError,
                    ):
                        continue

                    except Exception:
                        continue

            except asyncio.CancelledError:
                break

            except Exception:
                continue

    async def _run_offline_detection(
        self,
    ) -> None:
        """Run offline detection and automatic service recovery."""

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
                    node_id = node_info.node_id

                    self.registry.mark_offline(
                        node_id
                    )

                    self.resource_registry.remove_resources(
                        node_id
                    )

                    if not self._running:
                        break

                    await self._recover_services_from_node(
                        node_id
                    )

            except asyncio.CancelledError:
                break

            except Exception:
                continue