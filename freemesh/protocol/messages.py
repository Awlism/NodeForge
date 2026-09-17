"""Protocol message definitions for NodeForge."""

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel, Field


PROTOCOL_VERSION = "1.0"


class MessageType(str, Enum):
    """Supported NodeForge protocol message types."""

    REGISTER = "register"
    REGISTER_RESPONSE = "register_response"

    AUTHENTICATE = "authenticate"
    AUTHENTICATE_RESPONSE = "authenticate_response"

    HEARTBEAT = "heartbeat"
    HEARTBEAT_RESPONSE = "heartbeat_response"

    RESOURCE_REPORT = "resource_report"
    RESOURCE_REPORT_RESPONSE = "resource_report_response"

    SERVICE_START = "service_start"
    SERVICE_START_RESPONSE = "service_start_response"

    SERVICE_STOP = "service_stop"
    SERVICE_STOP_RESPONSE = "service_stop_response"

    SERVICE_STATUS = "service_status"
    SERVICE_STATUS_RESPONSE = "service_status_response"

    SERVICE_FAILURE = "service_failure"
    SERVICE_FAILURE_RESPONSE = "service_failure_response"

    STATUS = "status"
    ERROR = "error"
    DISCONNECT = "disconnect"


class BaseMessage(BaseModel):
    """Base protocol message exchanged between Controller and Node."""

    version: str = Field(default=PROTOCOL_VERSION)
    type: MessageType
    message_id: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )
    payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )