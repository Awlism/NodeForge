"""Node agent for the NodeForge distributed system."""

import asyncio
import socket
import uuid
from enum import Enum
from typing import Optional

from freemesh.node.resources import collect_node_resources
from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport
from freemesh.service import ServiceStatus
from freemesh.service_health import ServiceHealthChecker
from freemesh.service_manager import ServiceManager
from freemesh.service_requirements import ServiceRequirements


class AgentState(str, Enum):
    """Possible states of a NodeForge agent."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    ERROR = "error"


class NodeAgent:
    """Agent running on a NodeForge node."""

    def __init__(
        self,
        node_id: Optional[str] = None,
        hostname: Optional[str] = None,
        controller_host: str = "localhost",
        controller_port: int = 9999,
        authentication_token: Optional[str] = None,
        reconnect_delay_seconds: float = 5.0,
        heartbeat_interval_seconds: float = 5.0,
    ):
        self.node_id = node_id or str(uuid.uuid4())
        self.hostname = hostname or socket.gethostname()
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.authentication_token = authentication_token
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

        self.state = AgentState.DISCONNECTED
        self.transport = TCPTransport()

        self._running = False
        self._receive_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._service_monitor_task: Optional[asyncio.Task] = None

        self._service_manager = ServiceManager(
            max_restart_attempts=3
        )

        self._health_checker = ServiceHealthChecker()

    def _collect_resource_payload(self) -> dict:
        """Collect current resource information for this node."""

        resources = collect_node_resources(
            running_services=len(
                self._service_manager.list_services()
            ),
        )

        return {
            "node_id": self.node_id,
            "cpu_cores": resources.cpu_cores,
            "cpu_usage_percent": (
                resources.cpu_usage_percent
            ),
            "memory_total_mb": (
                resources.memory_total_mb
            ),
            "memory_used_mb": (
                resources.memory_used_mb
            ),
            "disk_total_gb": (
                resources.disk_total_gb
            ),
            "disk_used_gb": (
                resources.disk_used_gb
            ),
            "running_services": (
                resources.running_services
            ),
        }

    async def _send_resource_report(self) -> None:
        """Send the current resource information to the Controller."""

        resource_report = BaseMessage(
            type=MessageType.RESOURCE_REPORT,
            message_id=str(uuid.uuid4()),
            payload=self._collect_resource_payload(),
        )

        await self.transport.send(resource_report)

    async def start(self) -> None:
        """Start the node agent."""

        if self._running:
            return

        self._running = True

        while self._running:
            try:
                self.state = AgentState.CONNECTING

                self.transport = TCPTransport()

                await self.transport.connect(
                    self.controller_host,
                    self.controller_port,
                )

                await self._register_and_authenticate()

                self.state = AgentState.READY

                await self._send_resource_report()

                self._receive_task = asyncio.create_task(
                    self._receive_loop()
                )

                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop()
                )

                self._service_monitor_task = asyncio.create_task(
                    self._service_monitor_loop()
                )

                done, pending = await asyncio.wait(
                    [
                        self._receive_task,
                        self._heartbeat_task,
                        self._service_monitor_task,
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                for task in done:
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        pass

            except asyncio.CancelledError:
                break

            except Exception:
                self.state = AgentState.ERROR

            finally:
                await self.transport.disconnect()

                self._receive_task = None
                self._heartbeat_task = None
                self._service_monitor_task = None

            if self._running:
                self.state = AgentState.DISCONNECTED

                await asyncio.sleep(
                    self.reconnect_delay_seconds
                )

        self.state = AgentState.DISCONNECTED

    async def stop(self) -> None:
        """Stop the node agent."""

        self._running = False

        tasks = [
            self._receive_task,
            self._heartbeat_task,
            self._service_monitor_task,
        ]

        for task in tasks:
            if task is not None:
                task.cancel()

        await self._service_manager.stop_all()
        await self.transport.disconnect()

        self._receive_task = None
        self._heartbeat_task = None
        self._service_monitor_task = None

        self.state = AgentState.DISCONNECTED

    async def _register_and_authenticate(self) -> None:
        """Register the node and authenticate with the controller."""

        register_message = BaseMessage(
            type=MessageType.REGISTER,
            message_id=str(uuid.uuid4()),
            payload={
                "node_id": self.node_id,
                "hostname": self.hostname,
                "connection_address": self.controller_host,
                "connection_port": self.controller_port,
            },
        )

        await self.transport.send(register_message)

        register_response = await self.transport.receive()

        if register_response is None:
            raise ConnectionError(
                "Controller closed connection during registration"
            )

        if register_response.type == MessageType.ERROR:
            raise ConnectionError(
                register_response.payload.get(
                    "error",
                    "Registration failed",
                )
            )

        if register_response.type != MessageType.REGISTER_RESPONSE:
            raise ConnectionError(
                "Unexpected registration response"
            )

        self.state = AgentState.AUTHENTICATING

        auth_message = BaseMessage(
            type=MessageType.AUTHENTICATE,
            message_id=str(uuid.uuid4()),
            payload={
                "token": self.authentication_token,
            },
        )

        await self.transport.send(auth_message)

        auth_response = await self.transport.receive()

        if auth_response is None:
            raise ConnectionError(
                "Controller closed connection during authentication"
            )

        if (
            auth_response.type
            != MessageType.AUTHENTICATE_RESPONSE
        ):
            raise ConnectionError(
                "Unexpected authentication response"
            )

        if auth_response.payload.get("status") != "authenticated":
            raise PermissionError(
                auth_response.payload.get(
                    "error",
                    "Authentication failed",
                )
            )

    async def _receive_loop(self) -> None:
        """Receive messages from the controller."""

        while self._running:
            message = await self.transport.receive()

            if message is None:
                break

            if message.type == MessageType.HEARTBEAT_RESPONSE:
                continue

            if message.type == MessageType.RESOURCE_REPORT_RESPONSE:
                continue

            if message.type == MessageType.SERVICE_START:
                await self._handle_service_start(message)

            elif message.type == MessageType.SERVICE_STOP:
                await self._handle_service_stop(message)

            elif message.type == MessageType.SERVICE_STATUS:
                await self._handle_service_status(message)

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats and resource reports."""

        while self._running:
            await asyncio.sleep(
                self.heartbeat_interval_seconds
            )

            if not await self.transport.is_connected():
                break

            heartbeat = BaseMessage(
                type=MessageType.HEARTBEAT,
                message_id=str(uuid.uuid4()),
                payload={
                    "node_id": self.node_id,
                },
            )

            await self.transport.send(heartbeat)

            resource_report = BaseMessage(
                type=MessageType.RESOURCE_REPORT,
                message_id=str(uuid.uuid4()),
                payload=self._collect_resource_payload(),
            )

            await self.transport.send(resource_report)

    async def _service_monitor_loop(self) -> None:
        """Monitor service health and report crashes."""

        while self._running:
            await asyncio.sleep(0.5)

            for service in self._service_manager.list_services():
                process = self._service_manager.get_process(
                    service.service_id
                )

                if process is None:
                    continue

                if process.returncode is None:
                    self._health_checker.check(service)
                    continue

                service.mark_crashed()

                failure_message = BaseMessage(
                    type=MessageType.SERVICE_FAILURE,
                    message_id=str(uuid.uuid4()),
                    payload={
                        "service_id": service.service_id,
                        "node_id": self.node_id,
                        "command": service.command,
                        "status": ServiceStatus.CRASHED.value,
                        "restart_attempts": (
                            service.restart_attempts
                        ),
                        "max_restart_attempts": (
                            service.max_restart_attempts
                        ),
                        "requirements": (
                            service.requirements.to_dict()
                        ),
                        "health": service.health.value,
                        "error": "Service process exited",
                    },
                )

                try:
                    await self.transport.send(
                        failure_message
                    )
                except Exception:
                    pass

    async def _handle_service_start(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a service start request."""

        payload = message.payload

        service_id = payload.get("service_id")
        command = payload.get("command")
        request_id = payload.get(
            "request_id",
            message.message_id,
        )

        if not service_id or not command:
            response = BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": (
                        "service_id and command are required"
                    ),
                    "request_id": request_id,
                },
            )

            await self.transport.send(response)
            return

        try:
            requirements_payload = payload.get(
                "requirements",
                {},
            )

            if requirements_payload is None:
                requirements_payload = {}

            if not isinstance(
                requirements_payload,
                dict,
            ):
                raise TypeError(
                    "requirements must be an object"
                )

            requirements = (
                ServiceRequirements.from_dict(
                    requirements_payload
                )
            )

            service = await self._service_manager.start_service(
                service_id=service_id,
                command=command,
            )

            service.requirements = requirements
            service.node_id = self.node_id

            response = BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "started",
                    "service_id": service.service_id,
                    "node_id": self.node_id,
                    "command": service.command,
                    "pid": service.pid,
                    "restart_attempts": (
                        service.restart_attempts
                    ),
                    "max_restart_attempts": (
                        service.max_restart_attempts
                    ),
                    "requirements": (
                        service.requirements.to_dict()
                    ),
                    "request_id": request_id,
                },
            )

        except Exception as exc:
            response = BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "service_id": service_id,
                    "node_id": self.node_id,
                    "error": str(exc),
                    "request_id": request_id,
                },
            )

        await self.transport.send(response)

    async def _handle_service_stop(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a service stop request."""

        payload = message.payload

        service_id = payload.get("service_id")
        request_id = payload.get(
            "request_id",
            message.message_id,
        )

        if not service_id:
            response = BaseMessage(
                type=MessageType.SERVICE_STOP_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "error": "service_id is required",
                    "request_id": request_id,
                },
            )

            await self.transport.send(response)
            return

        try:
            service = await self._service_manager.stop_service(
                service_id
            )

            response = BaseMessage(
                type=MessageType.SERVICE_STOP_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": service.status.value,
                    "service_id": service.service_id,
                    "node_id": self.node_id,
                    "request_id": request_id,
                },
            )

        except Exception as exc:
            response = BaseMessage(
                type=MessageType.SERVICE_STOP_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "failed",
                    "service_id": service_id,
                    "node_id": self.node_id,
                    "error": str(exc),
                    "request_id": request_id,
                },
            )

        await self.transport.send(response)

    async def _handle_service_status(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a service status request."""

        payload = message.payload

        service_id = payload.get("service_id")
        request_id = payload.get(
            "request_id",
            message.message_id,
        )

        service = (
            self._service_manager.get_service(service_id)
            if service_id
            else None
        )

        process = (
            self._service_manager.get_process(service_id)
            if service_id
            else None
        )

        if service is None or process is None:
            response = BaseMessage(
                type=MessageType.SERVICE_STATUS_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "status": "not_found",
                    "service_id": service_id,
                    "node_id": self.node_id,
                    "request_id": request_id,
                },
            )

            await self.transport.send(response)
            return

        if service.status not in (
            ServiceStatus.CRASHED,
            ServiceStatus.FAILED,
        ):
            if process.returncode is None:
                service.mark_running(
                    pid=process.pid,
                    node_id=self.node_id,
                )

        self._health_checker.check(service)

        response = BaseMessage(
            type=MessageType.SERVICE_STATUS_RESPONSE,
            message_id=str(uuid.uuid4()),
            payload={
                "status": service.status.value,
                "health": service.health.value,
                "service_id": service.service_id,
                "node_id": self.node_id,
                "command": service.command,
                "pid": service.pid,
                "returncode": process.returncode,
                "restart_attempts": service.restart_attempts,
                "max_restart_attempts": (
                    service.max_restart_attempts
                ),
                "requirements": (
                    service.requirements.to_dict()
                ),
                "request_id": request_id,
            },
        )

        await self.transport.send(response)

    def get_state(self) -> AgentState:
        """Return the current agent state."""

        return self.state

    async def is_connected(self) -> bool:
        """Return whether the transport is connected."""

        return await self.transport.is_connected()

    def is_running(self) -> bool:
        """Return whether the agent is running."""

        return self._running