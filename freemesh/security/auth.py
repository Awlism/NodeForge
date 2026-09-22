"""Authentication and security abstractions for NodeForge."""

from __future__ import annotations

import hmac
import os
from abc import ABC, abstractmethod
from typing import Optional


class AuthenticationError(Exception):
    """Raised when authentication fails."""


class Authenticator(ABC):
    """Abstract base class for authentication implementations."""

    @abstractmethod
    def authenticate(
        self,
        credentials: str,
    ) -> bool:
        """Authenticate a credential string."""

        raise NotImplementedError


class DevelopmentTokenAuthenticator(Authenticator):
    """Token authenticator backed by an environment variable.

    This implementation remains intentionally simple for the current
    NodeForge development/runtime architecture. Production deployments
    should eventually use stronger identity mechanisms such as mTLS
    or a dedicated credential service.
    """

    ENV_VAR_NAME = "NODEFORGE_AUTH_TOKEN"

    def __init__(
        self,
        env_var: Optional[str] = None,
    ) -> None:
        self.env_var = (
            env_var or self.ENV_VAR_NAME
        )

        self.expected_token = os.getenv(
            self.env_var
        )

        if not self.expected_token:
            raise RuntimeError(
                f"Development authenticator requires "
                f"{self.env_var} environment variable "
                "to be set"
            )

    def authenticate(
        self,
        credentials: str,
    ) -> bool:
        """Authenticate a token using constant-time comparison."""

        if not credentials:
            raise AuthenticationError(
                "Credentials cannot be empty"
            )

        if not isinstance(
            credentials,
            str,
        ):
            raise AuthenticationError(
                "Credentials must be a string"
            )

        return self._constant_time_compare(
            credentials,
            self.expected_token,
        )

    @staticmethod
    def _constant_time_compare(
        a: str,
        b: str,
    ) -> bool:
        """Compare strings using constant-time equality."""

        if not isinstance(a, str):
            return False

        if not isinstance(b, str):
            return False

        return hmac.compare_digest(
            a.encode("utf-8"),
            b.encode("utf-8"),
        )