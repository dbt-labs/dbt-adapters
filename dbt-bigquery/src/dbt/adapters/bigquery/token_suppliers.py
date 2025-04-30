import requests
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional
from datetime import datetime, timedelta, timezone
from google.auth.identity_pool import SubjectTokenSupplier
from dbt.adapters.exceptions import FailedToConnectError
from google.auth.external_account import SupplierContext
from google.auth.transport import Request
from dbt_common.exceptions import DbtRuntimeError


class TokenServiceBase(ABC):
    def __init__(self, token_endpoint: Dict[str, Any]):
        expected_keys = {"type", "request_url", "request_data"}
        for key in expected_keys:
            if key not in token_endpoint:
                raise FailedToConnectError(f"Missing required key in token_endpoint: '{key}'")

        self.type: str = token_endpoint["type"]
        self.url: str = token_endpoint["request_url"]
        self.data: str = token_endpoint["request_data"]

        self.other_params = {k: v for k, v in token_endpoint.items() if k not in expected_keys}

    @abstractmethod
    def build_header_payload(self) -> Dict[str, Any]:
        pass

    def handle_request(self) -> requests.Response:
        """
        Handles the request with rate limiting and error handling.
        """
        response = requests.post(self.url, headers=self.build_header_payload(), data=self.data)

        if response.status_code == 429:
            raise DbtRuntimeError(
                "Rate limit on identity provider's token dispatch has been reached. "
                "Consider increasing your identity provider's refresh token rate or "
                "lower dbt's maximum concurrent thread count."
            )

        response.raise_for_status()
        return response


class EntraIdpTokenService(TokenServiceBase):
    """
    formatted based on docs: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#get-a-token
    """

    def build_header_payload(self) -> Dict[str, Any]:
        return {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        }


class TokenServiceType(Enum):
    ENTRA = "entra"


class EntraTokenSupplier(SubjectTokenSupplier):
    """
    A SubjectTokenSupplier that fetches tokens from Entra ID.

    This is an implementation of the SubjectTokenSupplier interface that can be specified as the
    credential_source field in a Google Cloud Application Default Credentials (ADC) config file.

    https://googleapis.dev/python/google-auth/latest/reference/google.auth.identity_pool.html#google.auth.identity_pool.SubjectTokenSupplier
    """

    def __init__(self, token_endpoint: Dict[str, Any]):
        self.token_service = EntraIdpTokenService(token_endpoint)
        self._cached_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        # Add a 5-minute buffer before actual expiry to ensure we don't use an expired token
        self._expiry_buffer = timedelta(minutes=5)

    def _is_token_valid(self) -> bool:
        """Check if the cached token is still valid."""
        if not self._token_expiry:
            return False
        return datetime.now(timezone.utc).replace(tzinfo=None) < (
            self._token_expiry - self._expiry_buffer
        )

    def _fetch_new_token(self) -> str:
        """Fetch a new token from Entra ID."""
        response = self.token_service.handle_request()
        token_data = response.json()

        if "access_token" not in token_data:
            raise FailedToConnectError(
                "access_token missing from Idp token request. Please confirm correct "
                "configuration of the token_endpoint field in profiles.yml and that your "
                "Idp can obtain an OIDC-compliant access token."
            )

        # Extract expiration time from token response
        # Default to 1 hour if not specified
        expires_in = token_data.get("expires_in", 3600)
        self._token_expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
            seconds=expires_in
        )
        self._cached_token = token_data["access_token"]

        return token_data["access_token"]

    def get_subject_token(self, context: SupplierContext, request: Request) -> str:
        """
        Fetch and return a subject token from Entra ID.

        Args:
            context: The context object containing information about the requested audience and subject token type
            request: The object used to make HTTP requests

        Returns:
            str: The access token from Entra ID

        Raises:
            FailedToConnectError: If token acquisition fails or response is invalid
        """
        if self._cached_token and self._is_token_valid():
            return self._cached_token

        return self._fetch_new_token()


def create_token_supplier(token_endpoint: Dict[str, Any]) -> SubjectTokenSupplier:
    if (service_type := token_endpoint.get("type")) is None:
        raise FailedToConnectError("Missing required key in token_endpoint: 'type'")

    if service_type == TokenServiceType.ENTRA.value:
        return EntraTokenSupplier(token_endpoint)
    else:
        raise ValueError(
            f"Unsupported identity provider type: {service_type}. Only 'entra' is supported at this time"
        )
