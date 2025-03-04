import requests
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any

from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtRuntimeError


# Define an Enum for the supported token endpoint types
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


def create_token_service_client(token_endpoint: Dict[str, Any]) -> TokenServiceBase:
    if (service_type := token_endpoint.get("type")) is None:
        raise FailedToConnectError("Missing required key in token_endpoint: 'type'")

    if service_type == TokenServiceType.ENTRA.value:
        return EntraIdpTokenService(token_endpoint)
    else:
        raise ValueError(
            f"Unsupported identity provider type: {service_type}. Only 'entra' is supported at this time"
        )
