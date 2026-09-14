"""Node agent for NodeForge."""

import asyncio
import os
import socket
import uuid
from enum import Enum
from typing import Optional

from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport


class AgentState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    ERROR = "error"


class NodeAgent:
    def __init__(
        self,
        node_id: str,
        hostname: Optional[str] = None,
        controller_host: str = "localhost",
        controller_port: int = 9999,
        authentication_token: Optional[str] = None,
        heartbeat_interval_seconds: float = 10.0,
        reconnect_delay_seconds: float = 5.0,
        max_restart_attempts: int = 3,
    ):
        self.node_id = node_id
        self.hostname = hostname or socket.gethostname()
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.authentication_token = (
            authentication_token
            or os.getenv("NODEFORGE_AUTH_TOKEN")
            or ""
        )

        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.max_restart_attempts = max(0, max_restart_attempts)

        self.state = AgentState.DISCONNECTED
        self.transport: Optional[TCPTransport] = None
        self._stop_event = asyncio.Event()

        self._service_processes: dict[str, asyncio.subprocess.Process] = {}
        self._service_commands: dict[str, str] = {}
        self._service_statuses: dict[str, str] = {}
        self._service_restart_attempts: dict[str, int] = {}

        self._service_monitor_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._stop_event.clear()

        if self._service_monitor_task is None:
            self._service_monitor_task = asyncio.create_task(
                self._service_monitor_loop()
            )

        while not self._stop_event.is_set():
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.state = AgentState.ERROR

            if not self._stop_event.is_set():
                self.state = AgentState.DISCONNECTED
                await asyncio.sleep(self.reconnect_delay_seconds)

    async def stop(self) -> None:
        self._stop_event.set()

        for service_id in list(self._service_processes.keys()):
            process = self._service_processes.get(service_id)

            if process is not None and process.returncode is None:
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except Exception:
                    try:
                        process.kill()
                        await process.wait()
                    except Exception:
                        pass

        self._service_processes.clear()
        self._service_commands.clear()
        self._service_statuses.clear()
        self._service_restart_attempts.clear()

        if self._service_monitor_task is not None:
            self._service_monitor_task.cancel()

            try:
                await self._service_monitor_task
            except asyncio.CancelledError:
                pass

            self._service_monitor_task = None

        if self.transport is not None:
            await self.transport.disconnect()
            self.transport = None

        self.state = AgentState.DISCONNECTED

    async def _connect_and_run(self) -> None:
        self.state = AgentState.CONNECTING

        self.transport = TCPTransport()
        await self.transport.connect(
            self.controller_host,
            self.controller_port,
        )

        await self._register_and_authenticate()

        self.state = AgentState.READY

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        receive_task = asyncio.create_task(self._receive_loop())

        done, pending = await asyncio.wait(
            {heartbeat_task, receive_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        for task in done:
            try:
                await task
            except asyncio.CancelledError:
                pass

        if self.transport is not None:
            await self.transport.disconnect()

        self.transport = None

        if not self._stop_event.is_set():
            self.state = AgentState.DISCONNECTED

    async def _register_and_authenticate(self) -> None:
        if self.transport is None:
            raise ConnectionError("Transport is not available")

        register_message = BaseMessage(
            type=MessageType.REGISTER,
            message_id=str(uuid.uuid4()),
            payload={
                "node_id": self.node_id,
                "hostname": self.hostname,
            },
        )

        await self.transport.send(register_message)

        response = await self.transport.receive()

        if response is None:
            raise ConnectionError("No register response received")

        if response.type != MessageType.REGISTER_RESPONSE:
            raise ConnectionError(
                f"Unexpected register response: {response.type}"
            )

        self.state = AgentState.AUTHENTICATING

        auth_message = BaseMessage(
            type=MessageType.AUTHENTICATE,
            message_id=str(uuid.uuid4()),
            payload={
                "node_id": self.node_id,
                "token": self.authentication_token,
            },
        )

        await self.transport.send(auth_message)

        response = await self.transport.receive()

        if response is None:
            raise ConnectionError("No authentication response received")

        if response.type != MessageType.AUTHENTICATE_RESPONSE:
            raise ConnectionError(
                f"Unexpected authentication response: {response.type}"
            )

        if not response.payload.get("authenticated", False):
            raise PermissionError("Node authentication failed")

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(self.heartbeat_interval_seconds)

            if self.transport is None:
                return

            heartbeat_message = BaseMessage(
                type=MessageType.HEARTBEAT,
                message_id=str(uuid.uuid4()),
                payload={
                    "node_id": self.node_id,
                },
            )

            await self.transport.send(heartbeat_message)

    async def _receive_loop(self) -> None:
        while not self._stop_event.is_set():
            if self.transport is None:
                return

            message = await self.transport.receive()

            if message is None:
                return

            if message.type == MessageType.SERVICE_START:
                await self._handle_service_start(message)

            elif message.type == MessageType.SERVICE_STOP:
                await self._handle_service_stop(message)

            elif message.type == MessageType.SERVICE_STATUS:
                await self._handle_service_status(message)

    async def _handle_service_start(self, message: BaseMessage) -> None:
        if self.transport is None:
            return

        service_id = message.payload.get("service_id")
        command = message.payload.get("command")
        request_id = message.payload.get(
            "request_id",
            message.message_id,
        )

        if not service_id or not command:
            await self._send_service_response(
                MessageType.ERROR,
                request_id,
                {
                    "error": "service_id and command are required",
                },
            )
            return

        existing = self._service_processes.get(service_id)

        if existing is not None and existing.returncode is None:
            await self._send_service_response(
                MessageType.SERVICE_START_RESPONSE,
                request_id,
                {
                    "service_id": service_id,
                    "status": "already_running",
                    "pid": existing.pid,
                },
            )
            return

        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )

            self._service_processes[service_id] = process
            self._service_commands[service_id] = command
            self._service_statuses[service_id] = "running"
            self._service_restart_attempts[service_id] = 0

            await self._send_service_response(
                MessageType.SERVICE_START_RESPONSE,
                request_id,
                {
                    "service_id": service_id,
                    "status": "started",
                    "pid": process.pid,
                },
            )

        except Exception as exc:
            self._service_statuses[service_id] = "crashed"

            await self._send_service_response(
                MessageType.ERROR,
                request_id,
                {
                    "service_id": service_id,
                    "error": str(exc),
                },
            )

    async def _handle_service_stop(self, message: BaseMessage) -> None:
        service_id = message.payload.get("service_id")
        request_id = message.payload.get(
            "request_id",
            message.message_id,
        )

        process = self._service_processes.get(service_id)

        if process is None:
            self._service_statuses[service_id] = "not_found"

            await self._send_service_response(
                MessageType.SERVICE_STOP_RESPONSE,
                request_id,
                {
                    "service_id": service_id,
                    "status": "not_found",
                },
            )
            return

        try:
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

            self._service_processes.pop(service_id, None)
            self._service_commands.pop(service_id, None)
            self._service_statuses[service_id] = "stopped"
            self._service_restart_attempts.pop(service_id, None)

            await self._send_service_response(
                MessageType.SERVICE_STOP_RESPONSE,
                request_id,
                {
                    "service_id": service_id,
                    "status": "stopped",
                },
            )

        except Exception as exc:
            await self._send_service_response(
                MessageType.ERROR,
                request_id,
                {
                    "service_id": service_id,
                    "error": str(exc),
                },
            )

    async def _handle_service_status(self, message: BaseMessage) -> None:
        service_id = message.payload.get("service_id")
        request_id = message.payload.get(
            "request_id",
            message.message_id,
        )

        process = self._service_processes.get(service_id)
        status = self._service_statuses.get(
            service_id,
            "not_found",
        )

        if process is not None:
            if process.returncode is None:
                status = "running"
            else:
                status = self._service_statuses.get(
                    service_id,
                    "crashed",
                )

        payload = {
            "service_id": service_id,
            "status": status,
            "pid": (
                process.pid
                if process is not None
                and process.returncode is None
                else None
            ),
            "restart_attempts": self._service_restart_attempts.get(
                service_id,
                0,
            ),
            "max_restart_attempts": self.max_restart_attempts,
        }

        await self._send_service_response(
            MessageType.SERVICE_STATUS_RESPONSE,
            request_id,
            payload,
        )

    async def _send_service_response(
        self,
        message_type: MessageType,
        request_id: str,
        payload: dict,
    ) -> None:
        if self.transport is None:
            return

        payload = {
            **payload,
            "request_id": request_id,
        }

        response = BaseMessage(
            type=message_type,
            message_id=str(uuid.uuid4()),
            payload=payload,
        )

        await self.transport.send(response)

    async def _service_monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(0.1)

            for service_id, process in list(
                self._service_processes.items()
            ):
                if process.returncode is None:
                    continue

                current_status = self._service_statuses.get(
                    service_id
                )

                if current_status != "running":
                    continue

                self._service_statuses[service_id] = "crashed"

                command = self._service_commands.get(service_id)

                if not command:
                    continue

                attempts = self._service_restart_attempts.get(
                    service_id,
                    0,
                )

                if attempts >= self.max_restart_attempts:
                    continue

                attempts += 1
                self._service_restart_attempts[service_id] = attempts

                try:
                    new_process = await asyncio.create_subprocess_shell(
                        command,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )

                    self._service_processes[service_id] = new_process
                    self._service_statuses[service_id] = "running"

                except Exception:
                    self._service_statuses[service_id] = "crashed"

    def get_state(self) -> AgentState:
        return self.state