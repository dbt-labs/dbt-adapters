import os
from dataclasses import dataclass
from typing import Optional, Tuple

from dbt.adapters.contracts.connection import Credentials
from dbt_common.dataclass_schema import StrEnum

DEFAULT_BASE_URL = "https://api.lake-compute.fivetran.com/"
BASE_URL_ENV_VAR = "DBT_COMPUTE_BASE_URL"


class LakeComputeAuthMethod(StrEnum):
    TOKEN = "token"
    API_KEY = "api_key"
    OKTA_BROWSER = "okta_browser"
    FIVETRAN = "fivetran"


@dataclass
class LakeComputeCredentials(Credentials):
    """Profile fields for a ``type: lakecompute`` target.

    Which auth fields are required depends on ``method``, so they are validated at connect
    time (see ``auth.validate_auth_credentials``), where ``dbt debug`` can report them.
    """

    base_url: Optional[str] = None
    method: str = LakeComputeAuthMethod.TOKEN

    token: Optional[str] = None
    api_key: Optional[str] = None
    okta_auth_url: Optional[str] = None
    okta_token_url: Optional[str] = None
    okta_client_id: Optional[str] = None
    fivetran_credential: Optional[str] = None
    fivetran_auth_url: Optional[str] = None

    _ALIASES = {"dbname": "database"}

    @property
    def type(self) -> str:
        return "lakecompute"

    @property
    def unique_field(self) -> str:
        return self.resolved_base_url

    @property
    def resolved_base_url(self) -> str:
        """profile -> $DBT_COMPUTE_BASE_URL -> public endpoint."""
        return self.base_url or os.getenv(BASE_URL_ENV_VAR) or DEFAULT_BASE_URL

    def _connection_keys(self) -> Tuple[str, ...]:
        # secrets (token, api_key, fivetran_credential) are deliberately absent
        return (
            "base_url",
            "database",
            "schema",
            "method",
            "okta_auth_url",
            "okta_token_url",
            "okta_client_id",
            "fivetran_auth_url",
        )
