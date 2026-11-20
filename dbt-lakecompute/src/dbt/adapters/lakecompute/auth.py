"""Mapping from the profile's ``method`` to the driver's ``adbc.dbt.auth.*`` options."""

from typing import Dict, TYPE_CHECKING

from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.lakecompute.credentials import LakeComputeAuthMethod

if TYPE_CHECKING:
    from dbt.adapters.lakecompute.credentials import LakeComputeCredentials


OPTION_AUTH_METHOD = "adbc.dbt.auth.method"
OPTION_AUTH_TOKEN = "adbc.dbt.auth.token"
OPTION_AUTH_API_KEY = "adbc.dbt.auth.api_key"
OPTION_AUTH_OKTA_AUTH_URL = "adbc.dbt.auth.okta.auth_url"
OPTION_AUTH_OKTA_TOKEN_URL = "adbc.dbt.auth.okta.token_url"
OPTION_AUTH_OKTA_CLIENT_ID = "adbc.dbt.auth.okta.client_id"
OPTION_AUTH_FIVETRAN_CREDENTIAL = "adbc.dbt.auth.fivetran.credential"
OPTION_AUTH_FIVETRAN_API_URL = "adbc.dbt.auth.fivetran.api_url"


REQUIRED_FIELD_BY_METHOD: Dict[str, str] = {
    LakeComputeAuthMethod.TOKEN: "token",
    LakeComputeAuthMethod.API_KEY: "api_key",
    LakeComputeAuthMethod.FIVETRAN: "fivetran_credential",
    # okta_browser needs no pre-shared secret: the browser flow supplies it
    LakeComputeAuthMethod.OKTA_BROWSER: "",
}


def validate_auth_credentials(credentials: "LakeComputeCredentials") -> None:
    """Raise if ``method`` is unknown or its required field is missing."""
    method = credentials.method

    if method not in REQUIRED_FIELD_BY_METHOD:
        supported = ", ".join(sorted(REQUIRED_FIELD_BY_METHOD))
        raise FailedToConnectError(
            f"Unknown lakecompute auth method {method!r}. Supported methods: {supported}."
        )

    required = REQUIRED_FIELD_BY_METHOD[method]
    if required and not getattr(credentials, required, None):
        raise FailedToConnectError(
            f"lakecompute auth method {method!r} requires `{required}` to be set in the profile."
        )


def auth_options(credentials: "LakeComputeCredentials") -> Dict[str, str]:
    """Build the ``adbc.dbt.auth.*`` database options for this profile.

    Optional fields are emitted only when present: the driver distinguishes "unset" from
    "set to empty".
    """
    validate_auth_credentials(credentials)

    method = credentials.method
    options: Dict[str, str] = {OPTION_AUTH_METHOD: str(method)}

    if method == LakeComputeAuthMethod.TOKEN:
        options[OPTION_AUTH_TOKEN] = str(credentials.token)

    elif method == LakeComputeAuthMethod.API_KEY:
        options[OPTION_AUTH_API_KEY] = str(credentials.api_key)

    elif method == LakeComputeAuthMethod.OKTA_BROWSER:
        _set_if_present(options, OPTION_AUTH_OKTA_AUTH_URL, credentials.okta_auth_url)
        _set_if_present(options, OPTION_AUTH_OKTA_TOKEN_URL, credentials.okta_token_url)
        _set_if_present(options, OPTION_AUTH_OKTA_CLIENT_ID, credentials.okta_client_id)

    elif method == LakeComputeAuthMethod.FIVETRAN:
        options[OPTION_AUTH_FIVETRAN_CREDENTIAL] = str(credentials.fivetran_credential)
        _set_if_present(options, OPTION_AUTH_FIVETRAN_API_URL, credentials.fivetran_auth_url)

    return options


def _set_if_present(options: Dict[str, str], key: str, value: object) -> None:
    if value is not None and value != "":
        options[key] = str(value)
