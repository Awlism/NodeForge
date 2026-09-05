"""Authentication and security abstractions for NodeForge."""

import os
from abc import ABC, abstractmethod
from typing import Optional


class AuthenticationError(Exception):
    """Raised when authentication fails."""

    pass


class Authenticator(ABC):
    """Abstract base class for authentication implementations."""

    @abstractmethod
    def authenticate(self, credentials: str) -> bool:
        """Authenticate a credential string.

        Args:
            credentials: Credential string to authenticate

        Returns:
            True if authentication succeeds, False otherwise

        Raises:
            AuthenticationError: If authentication fails or is invalid
        """
        pass


class DevelopmentTokenAuthenticator(Authenticator):
    """Development-only token authenticator using environment configuration.

    This authenticator reads the expected token from the NODEFORGE_AUTH_TOKEN
    environment variable. It is suitable only for development and testing.

    Production deployments should use a stronger authentication mechanism
    such as TLS/mTLS, credential rotation, or a dedicated identity service.
    """

    ENV_VAR_NAME = "NODEFORGE_AUTH_TOKEN"

    def __init__(self, env_var: Optional[str] = None):
        """Initialize the development token authenticator.

        Args:
            env_var: Optional custom environment variable name.
                     Defaults to NODEFORGE_AUTH_TOKEN.

        Raises:
            RuntimeError: If the expected token is not configured in environment
        """
        self.env_var = env_var or self.ENV_VAR_NAME
        self.expected_token = os.getenv(self.env_var)

        if not self.expected_token:
            raise RuntimeError(
                f"Development authenticator requires {self.env_var} "
                "environment variable to be set"
            )

    def authenticate(self, credentials: str) -> bool:
        """Authenticate a token against the configured expected token.

        Args:
            credentials: Token string to authenticate

        Returns:
            True if token matches the configured expected token

        Raises:
            AuthenticationError: If credentials are invalid or empty
        """
        if not credentials:
            raise AuthenticationError("Credentials cannot be empty")

        if not isinstance(credentials, str):
            raise AuthenticationError("Credentials must be a string")

        # Simple constant-time comparison (timing-safe)
        return self._constant_time_compare(credentials, self.expected_token)

    @staticmethod
    def _constant_time_compare(a: str, b: str) -> bool:
        """Compare two strings in constant time to prevent timing attacks.

        Args:
            a: First string
            b: Second string

        Returns:
            True if strings are equal, False otherwise
        """
        if len(a) != len(b):
            return False

        result = 0
        for x, y in zip(a, b):
            result |= ord(x) ^ ord(y)

        return result == 0
