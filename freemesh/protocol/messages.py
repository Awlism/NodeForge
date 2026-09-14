"""Protocol message definitions for NodeForge."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


PROTOCOL_VERSION = "1.0"


class MessageType(str, Enum):
    """Enumeration of all message types in the protocol."""

    REGISTER = "register"
    REGISTER_RESPONSE = "register_response"
    AUTHENTICATE = "authenticate"
    AUTHENTICATE_RESPONSE = "authenticate_response"
    HEARTBEAT = "heartbeat"
    HEARTBEAT_RESPONSE = "heartbeat_response"

    SERVICE_START = "service_start"
    SERVICE_START_RESPONSE = "service_start_response"
    SERVICE_STOP = "service_stop"
    SERVICE_STOP_RESPONSE = "service_stop_response"
    SERVICE_STATUS = "service_status"
    SERVICE_STATUS_RESPONSE = "service_status_response"

    STATUS = "status"
    ERROR = "error"
    DISCONNECT = "disconnect"


class BaseMessage(BaseModel):
    """Base message model for all protocol messages."""

    version: str = Field(
        default=PROTOCOL_VERSION,
        description="Protocol version",
    )
    type: MessageType = Field(
        ...,
        description="Message type",
    )
    message_id: str = Field(
        ...,
        description="Unique message identifier",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Message payload",
    )
    timestamp: Optional[float] = Field(
        default=None,
        description="Message timestamp",
    )

    model_config = ConfigDict(use_enum_values=False)