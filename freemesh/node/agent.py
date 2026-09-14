"""Node agent for connecting to the NodeForge controller."""

import asyncio
import uuid
from enum import Enum
from typing import Optional

from freemesh.protocol.messages import BaseMessage, MessageType
from freemesh.protocol.transport import TCPTransport


class AgentState(str, Enum):
    """Enumeration of possible agent states."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    REGISTERING = "registering"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    ERROR = "error"


class NodeAgent:
    """Node agent for communicating with the NodeForge controller."""

    def __init__(
        self,
        node_id: str,
        hostname: str,
        controller_host: str = "localhost",
        controller_port: int = 9999,
        authentication_token: str = "",
        heartbeat_interval_seconds: float = 10.0,
        reconnect_delay_seconds: float = 5.0,
    ):
        self.node_id = node_id
        self.hostname = hostname
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.authentication_token = authentication_token
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.reconnect_delay_seconds = reconnect_delay_seconds

        self.transport: Optional[TCPTransport] = None
        self.state = AgentState.DISCONNECTED
        self._running = False

        self._receiver_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._response_waiters: dict[str, asyncio.Future] = {}
        self._services: dict[str, asyncio.subprocess.Process] = {}

    async def start(self) -> None:
        """Start the node agent and connect to the controller."""

        if self._running:
            raise RuntimeError("Node agent is already running")

        self._running = True
        self.state = AgentState.CONNECTING

        while self._running:
            try:
                self.transport = TCPTransport()

                await self.transport.connect(
                    self.controller_host,
                    self.controller_port,
                )

                success = await self._register_and_authenticate()

                if not success:
                    self.state = AgentState.ERROR
                    await self.transport.disconnect()

                    if self._running:
                        await asyncio.sleep(self.reconnect_delay_seconds)

                    continue

                self.state = AgentState.READY

                self._receiver_task = asyncio.create_task(
                    self._receive_loop()
                )

                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop()
                )

                done, pending = await asyncio.wait(
                    [
                        self._receiver_task,
                        self._heartbeat_task,
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                for task in pending:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                self._receiver_task = None
                self._heartbeat_task = None

                if self._running:
                    self.state = AgentState.ERROR

                    if self.transport:
                        await self.transport.disconnect()

                    await asyncio.sleep(self.reconnect_delay_seconds)

            except Exception:
                self.state = AgentState.ERROR

                if self.transport:
                    await self.transport.disconnect()

                if self._running:
                    await asyncio.sleep(self.reconnect_delay_seconds)

        self.state = AgentState.DISCONNECTED

    async def stop(self) -> None:
        """Stop the node agent and disconnect from the controller."""

        self._running = False
        self.state = AgentState.DISCONNECTED

        for task in (
            self._receiver_task,
            self._heartbeat_task,
        ):
            if task and not task.done():
                task.cancel()

        for task in (
            self._receiver_task,
            self._heartbeat_task,
        ):
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._receiver_task = None
        self._heartbeat_task = None

        await self._stop_all_services()

        for future in self._response_waiters.values():
            if not future.done():
                future.cancel()

        self._response_waiters.clear()

        if self.transport:
            await self.transport.disconnect()
            self.transport = None

    async def _register_and_authenticate(self) -> bool:
        """Perform registration and authentication with the controller."""

        if self.transport is None:
            return False

        try:
            self.state = AgentState.REGISTERING

            register_message = BaseMessage(
                type=MessageType.REGISTER,
                message_id=str(uuid.uuid4()),
                payload={
                    "node_id": self.node_id,
                    "hostname": self.hostname,
                },
            )

            await self.transport.send(register_message)

            register_response = await self.transport.receive()

            if register_response is None:
                return False

            if register_response.type != MessageType.REGISTER_RESPONSE:
                return False

            payload = register_response.payload

            if not isinstance(payload, dict):
                return False

            if payload.get("status") != "registered":
                return False

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
                return False

            if auth_response.type != MessageType.AUTHENTICATE_RESPONSE:
                return False

            payload = auth_response.payload

            if not isinstance(payload, dict):
                return False

            if payload.get("status") != "authenticated":
                return False

            return True

        except Exception:
            return False

    async def _receive_loop(self) -> None:
        """Receive and dispatch messages from the controller."""

        while self._running and self.state == AgentState.READY:
            try:
                if self.transport is None:
                    return

                message = await self.transport.receive()

                if message is None:
                    return

                if message.type == MessageType.HEARTBEAT_RESPONSE:
                    self._resolve_response(message)
                    continue

                if message.type == MessageType.SERVICE_START:
                    await self._handle_service_start(message)
                    continue

                if message.type == MessageType.SERVICE_STOP:
                    await self._handle_service_stop(message)
                    continue

                self._resolve_response(message)

            except asyncio.CancelledError:
                return
            except Exception:
                return

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat messages."""

        while self._running and self.state == AgentState.READY:
            try:
                await asyncio.sleep(self.heartbeat_interval_seconds)

                if not self._running or self.state != AgentState.READY:
                    return

                if self.transport is None:
                    return

                message_id = str(uuid.uuid4())

                heartbeat_message = BaseMessage(
                    type=MessageType.HEARTBEAT,
                    message_id=message_id,
                    payload={
                        "node_id": self.node_id,
                    },
                )

                await self.transport.send(heartbeat_message)

            except asyncio.CancelledError:
                return
            except Exception:
                return

    def _resolve_response(self, message: BaseMessage) -> None:
        """Resolve a pending response future."""

        future = self._response_waiters.pop(
            message.message_id,
            None,
        )

        if future and not future.done():
            future.set_result(message)

    async def _handle_service_start(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a request to start a service."""

        if self.transport is None:
            return

        payload = message.payload

        if not isinstance(payload, dict):
            return

        service_id = payload.get("service_id")
        command = payload.get("command")

        if not service_id or not command:
            response = BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "service_id": service_id,
                    "status": "error",
                    "error": "service_id and command are required",
                },
            )

            await self.transport.send(response)
            return

        if service_id in self._services:
            process = self._services[service_id]

            if process.returncode is None:
                response = BaseMessage(
                    type=MessageType.SERVICE_START_RESPONSE,
                    message_id=str(uuid.uuid4()),
                    payload={
                        "service_id": service_id,
                        "status": "already_running",
                    },
                )

                await self.transport.send(response)
                return

            del self._services[service_id]

        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )

            self._services[service_id] = process

            response = BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "service_id": service_id,
                    "status": "started",
                    "pid": process.pid,
                },
            )

        except Exception as exc:
            response = BaseMessage(
                type=MessageType.SERVICE_START_RESPONSE,
                message_id=str(uuid.uuid4()),
                payload={
                    "service_id": service_id,
                    "status": "error",
                    "error": str(exc),
                },
            )

        await self.transport.send(response)

    async def _handle_service_stop(
        self,
        message: BaseMessage,
    ) -> None:
        """Handle a request to stop a service."""

        if self.transport is None:
            return

        payload = message.payload

        if not isinstance(payload, dict):
            return

        service_id = payload.get("service_id")

        if not service_id:
            return

        process = self._services.get(service_id)

        if process is None:
            status = "not_running"
        elif process.returncode is not None:
            self._services.pop(service_id, None)
            status = "not_running"
        else:
            process.terminate()

            try:
                await asyncio.wait_for(
                    process.wait(),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

            self._services.pop(service_id, None)
            status = "stopped"

        response = BaseMessage(
            type=MessageType.SERVICE_STOP_RESPONSE,
            message_id=str(uuid.uuid4()),
            payload={
                "service_id": service_id,
                "status": status,
            },
        )

        await self.transport.send(response)

    async def _stop_all_services(self) -> None:
        """Stop all services managed by this node."""

        for service_id, process in list(self._services.items()):
            if process.returncode is None:
                try:
                    process.terminate()

                    await asyncio.wait_for(
                        process.wait(),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                except Exception:
                    pass

            self._services.pop(service_id, None)

    def get_state(self) -> AgentState:
        """Get the current state of the agent."""

        return self.state

    def is_connected(self) -> bool:
        """Check if the agent is connected and authenticated."""

        return self.state == AgentState.READY

    def is_running(self) -> bool:
        """Check if the agent is currently running."""

        return self._running