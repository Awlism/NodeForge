"""Node agent for NodeForge."""

import asyncio
import socket
import uuid
from enum import Enum
from typing import Any, Dict, Optional

from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport
from freemesh.service import Service, ServiceStatus


class AgentState(str, Enum):
    """States of a NodeAgent."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    REGISTERING = "registering"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    ERROR = "error"


class NodeAgent:
    """Agent that connects a NodeForge node to a controller."""

    def __init__(
        self,
        node_id: str,
        hostname: Optional[str] = None,
        controller_host: str = "127.0.0.1",
        controller_port: int = 8000,
        authentication_token: Optional[str] = None,
        heartbeat_interval_seconds: float = 10.0,
        reconnect_delay_seconds: float = 5.0,
    ):
        self.node_id = node_id
        self.hostname = hostname or socket.gethostname()
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.authentication_token = authentication_token

        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.reconnect_delay_seconds = reconnect_delay_seconds

        self.state = AgentState.DISCONNECTED
        self.transport: Optional[TCPTransport] = None

        self._running = False
        self._receiver_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._service_monitor_task: Optional[asyncio.Task] = None

        self._services: Dict[str, asyncio.subprocess.Process] = {}
        self._service_models: Dict[str, Service] = {}

        self._max_service_restart_attempts = 3

        self._response_waiters: Dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        """Start the node agent and keep reconnecting until stopped."""
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

                authenticated = await self._register_and_authenticate()

                if not authenticated:
                    self.state = AgentState.ERROR

                    if self.transport is not None:
                        await self.transport.disconnect()

                    self.transport = None

                    if self._running:
                        await asyncio.sleep(
                            self.reconnect_delay_seconds
                        )

                    continue

                self.state = AgentState.READY

                self._receiver_task = asyncio.create_task(
                    self._receive_loop()
                )

                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop()
                )

                self._service_monitor_task = asyncio.create_task(
                    self._service_monitor_loop()
                )

                tasks = {
                    self._receiver_task,
                    self._heartbeat_task,
                    self._service_monitor_task,
                }

                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                if pending:
                    await asyncio.gather(
                        *pending,
                        return_exceptions=True,
                    )

                for task in done:
                    if not task.cancelled():
                        exception = task.exception()

                        if exception is not None:
                            raise exception

            except asyncio.CancelledError:
                break

            except Exception:
                self.state = AgentState.ERROR

            finally:
                self._cancel_background_tasks()

                if self.transport is not None:
                    try:
                        await self.transport.disconnect()
                    except Exception:
                        pass

                self.transport = None

                if self._running:
                    self.state = AgentState.DISCONNECTED

            if self._running:
                await asyncio.sleep(
                    self.reconnect_delay_seconds
                )

        self.state = AgentState.DISCONNECTED

    async def stop(self) -> None:
        """Stop the node agent and all running services."""
        if not self._running:
            return

        self._running = False

        self._cancel_background_tasks()

        await self._stop_all_services()

        for future in self._response_waiters.values():
            if not future.done():
                future.cancel()

        self._response_waiters.clear()

        if self.transport is not None:
            try:
                await self.transport.disconnect()
            except Exception:
                pass

        self.transport = None
        self.state = AgentState.DISCONNECTED

    def _cancel_background_tasks(self) -> None:
        """Cancel agent background tasks."""
        tasks = (
            self._receiver_task,
            self._heartbeat_task,
            self._service_monitor_task,
        )

        for task in tasks:
            if task is not None and not task.done():
                task.cancel()

        self._receiver_task = None
        self._heartbeat_task = None
        self._service_monitor_task = None

    async def _register_and_authenticate(self) -> bool:
        """Register this node and authenticate with the controller."""
        if self.transport is None:
            return False

        self.state = AgentState.REGISTERING

        registration_message = BaseMessage(
            type=MessageType.REGISTER,
            message_id=str(uuid.uuid4()),
            payload={
                "node_id": self.node_id,
                "hostname": self.hostname,
                "connection_address": self.controller_host,
                "connection_port": self.controller_port,
            },
        )

        await self.transport.send(registration_message)

        response = await self.transport.receive()

        if response is None:
            return False

        if response.type == MessageType.ERROR:
            return False

        if response.type != MessageType.REGISTER_RESPONSE:
            return False

        if response.payload.get("status") != "registered":
            return False

        self.state = AgentState.AUTHENTICATING

        authentication_message = BaseMessage(
            type=MessageType.AUTHENTICATE,
            message_id=str(uuid.uuid4()),
            payload={
                "node_id": self.node_id,
                "token": self.authentication_token or "",
            },
        )

        await self.transport.send(authentication_message)

        response = await self.transport.receive()

        if response is None:
            return False

        if response.type == MessageType.ERROR:
            return False

        if response.type != MessageType.AUTHENTICATE_RESPONSE:
            return False

        return response.payload.get("status") == "authenticated"

    async def _receive_loop(self) -> None:
        """Receive and process messages from the controller."""
        if self.transport is None:
            return

        while self._running and await self.transport.is_connected():
            message = await self.transport.receive()

            if message is None:
                break

            if message.type == MessageType.HEARTBEAT_RESPONSE:
                continue

            if message.type == MessageType.SERVICE_START:
                await self._handle_service_start(message)
                continue

            if message.type == MessageType.SERVICE_STOP:
                await self._handle_service_stop(message)
                continue

            if message.type == MessageType.SERVICE_STATUS:
                await self._handle_service_status(message)
                continue

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to the controller."""
        while self._running:
            await asyncio.sleep(
                self.heartbeat_interval_seconds
            )

            if not self._running:
                break

            if self.transport is None:
                break

            if not await self.transport.is_connected():
                break

            heartbeat_message = BaseMessage(
                type=MessageType.HEARTBEAT,
                message_id=str(uuid.uuid4()),
                payload={
                    "node_id": self.node_id,
                },
            )

            await self.transport.send(heartbeat_message)

    async def _service_monitor_loop(self) -> None:
        """Monitor running services and restart crashed services."""
        while self._running:
            await asyncio.sleep(0.5)

            for service_id, process in list(
                self._services.items()
            ):
                service = self._service_models.get(service_id)

                if service is None:
                    continue

                if process.returncode is None:
                    continue

                service.mark_crashed()

                if (
                    service.restart_attempts
                    >= service.max_restart_attempts
                ):
                    if self.transport is not None:
                        failure_message = BaseMessage(
                            type=MessageType.SERVICE_FAILURE,
                            message_id=str(uuid.uuid4()),
                            payload={
                                "service_id": service_id,
                                "status": ServiceStatus.CRASHED.value,
                                "restart_attempts": (
                                    service.restart_attempts
                                ),
                                "max_restart_attempts": (
                                    service.max_restart_attempts
                                ),
                                "reason": (
                                    "maximum restart attempts reached"
                                ),
                            },
                        )

                        await self.transport.send(
                            failure_message
                        )

                    continue

                command = service.command

                if not command:
                    continue

                try:
                    new_process = (
                        await asyncio.create_subprocess_shell(
                            command
                        )
                    )

                    self._services[service_id] = new_process

                    service.restart_attempts += 1

                    if (
                        service.restart_attempts
                        >= service.max_restart_attempts
                    ):
                        service.mark_crashed()
                    else:
                        service.mark_running(
                            pid=new_process.pid
                        )

                except Exception:
                    service.mark_failed()

    async def _handle_service_start(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a service start request."""
        service_id = message.payload.get("service_id")
        command = message.payload.get("command")

        if not service_id or not command:
            await self._send_service_response(
                MessageType.SERVICE_START_RESPONSE,
                message.message_id,
                {
                    "status": "failed",
                    "reason": (
                        "service_id and command are required"
                    ),
                },
            )
            return

        existing_process = self._services.get(
            service_id
        )

        if existing_process is not None:
            if existing_process.returncode is None:
                await self._send_service_response(
                    MessageType.SERVICE_START_RESPONSE,
                    message.message_id,
                    {
                        "service_id": service_id,
                        "status": "already_running",
                        "pid": existing_process.pid,
                    },
                )
                return

        service = Service(
            service_id=service_id,
            command=command,
            max_restart_attempts=(
                self._max_service_restart_attempts
            ),
        )

        self._service_models[service_id] = service

        try:
            service.mark_starting()

            process = (
                await asyncio.create_subprocess_shell(
                    command
                )
            )

            self._services[service_id] = process

            service.mark_running(
                pid=process.pid
            )

            await self._send_service_response(
                MessageType.SERVICE_START_RESPONSE,
                message.message_id,
                {
                    "service_id": service_id,
                    "status": "started",
                    "pid": process.pid,
                },
            )

        except Exception as exc:
            service.mark_failed()

            await self._send_service_response(
                MessageType.SERVICE_START_RESPONSE,
                message.message_id,
                {
                    "service_id": service_id,
                    "status": "failed",
                    "reason": str(exc),
                },
            )

    async def _handle_service_stop(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a service stop request."""
        service_id = message.payload.get("service_id")

        if not service_id:
            await self._send_service_response(
                MessageType.SERVICE_STOP_RESPONSE,
                message.message_id,
                {
                    "status": "failed",
                    "reason": "service_id is required",
                },
            )
            return

        process = self._services.get(service_id)
        service = self._service_models.get(service_id)

        if process is None or service is None:
            await self._send_service_response(
                MessageType.SERVICE_STOP_RESPONSE,
                message.message_id,
                {
                    "service_id": service_id,
                    "status": "not_found",
                },
            )
            return

        try:
            service.mark_stopping()

            if process.returncode is None:
                process.terminate()

                try:
                    await asyncio.wait_for(
                        process.wait(),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()

            service.mark_stopped()

            self._services.pop(service_id, None)
            self._service_models.pop(service_id, None)

            await self._send_service_response(
                MessageType.SERVICE_STOP_RESPONSE,
                message.message_id,
                {
                    "service_id": service_id,
                    "status": "stopped",
                },
            )

        except Exception as exc:
            service.mark_failed()

            await self._send_service_response(
                MessageType.SERVICE_STOP_RESPONSE,
                message.message_id,
                {
                    "service_id": service_id,
                    "status": "failed",
                    "reason": str(exc),
                },
            )

    async def _handle_service_status(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a service status request."""
        service_id = message.payload.get("service_id")

        if not service_id:
            await self._send_service_response(
                MessageType.SERVICE_STATUS_RESPONSE,
                message.message_id,
                {
                    "status": "failed",
                    "reason": "service_id is required",
                },
            )
            return

        process = self._services.get(service_id)
        service = self._service_models.get(service_id)

        if process is None or service is None:
            await self._send_service_response(
                MessageType.SERVICE_STATUS_RESPONSE,
                message.message_id,
                {
                    "service_id": service_id,
                    "status": "not_found",
                    "pid": None,
                    "restart_attempts": 0,
                },
            )
            return

        if service.status == ServiceStatus.CRASHED:
            status = ServiceStatus.CRASHED.value
        elif service.status == ServiceStatus.FAILED:
            status = ServiceStatus.FAILED.value
        elif process.returncode is None:
            status = ServiceStatus.RUNNING.value
            service.status = ServiceStatus.RUNNING
        else:
            status = service.status.value

        await self._send_service_response(
            MessageType.SERVICE_STATUS_RESPONSE,
            message.message_id,
            {
                "service_id": service_id,
                "status": status,
                "pid": process.pid,
                "returncode": process.returncode,
                "restart_attempts": (
                    service.restart_attempts
                ),
                "max_restart_attempts": (
                    service.max_restart_attempts
                ),
            },
        )

    async def _send_service_response(
        self,
        message_type: MessageType,
        message_id: str,
        payload: Dict[str, Any],
    ) -> None:
        """Send a service response to the controller."""
        if self.transport is None:
            return

        response = BaseMessage(
            type=message_type,
            message_id=message_id,
            payload=payload,
        )

        await self.transport.send(response)

    async def _stop_all_services(self) -> None:
        """Stop all running services."""
        for service_id, process in list(
            self._services.items()
        ):
            service = self._service_models.get(service_id)

            try:
                if service is not None:
                    service.mark_stopping()

                if process.returncode is None:
                    process.terminate()

                    try:
                        await asyncio.wait_for(
                            process.wait(),
                            timeout=2.0,
                        )
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()

                if service is not None:
                    service.mark_stopped()

            except Exception:
                if service is not None:
                    service.mark_failed()

            self._services.pop(service_id, None)
            self._service_models.pop(service_id, None)

    def get_state(self) -> AgentState:
        """Return the current agent state."""
        return self.state

    def is_connected(self) -> bool:
        """Return whether the agent is ready and connected."""
        return self.state == AgentState.READY

    def is_running(self) -> bool:
        """Return whether the agent is running."""
        return self._running