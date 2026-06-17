"""Dynamic workload identity token providers for Snowflake WIF/OIDC auth.

dbt-snowflake can mint a fresh workload identity token immediately before each
connection is opened, instead of relying on a static ``token`` that can expire
before dbt opens a *later* connection (e.g. short-lived CI OIDC tokens on long
or highly-parallel runs, where late-starting threads, connection-pool misses, or
reconnects open connections after the source token's TTL). The token is consumed
only at connection open; an already-open connection is kept alive by Snowflake's
own session/master tokens and does not need the source token re-minted.

Adding a provider:
    1. write a ``_mint_<provider>(config) -> str`` function below
    2. register it in ``_PROVIDERS``

``connections.py`` never needs to change to add a provider. Each provider is
responsible for returning a valid token string or raising a dbt error. Tokens
and raw provider responses must never be logged.
"""

import os
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import requests

from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError


GITHUB_OIDC_DEFAULT_AUDIENCE = "snowflakecomputing.com"
# GitHub Actions OIDC tokens are only ever requested from this host family. The
# request carries a bearer token that can itself mint identity tokens, so we
# refuse to send it anywhere else even if the request-URL env var is tampered with.
_GITHUB_OIDC_ALLOWED_HOST = "actions.githubusercontent.com"


@dataclass
class WorkloadIdentityTokenConfig(dbtClassMixin):
    """How to mint a fresh workload identity token at connection-open time.

    This is intentionally *not* a token value -- it names the provider dbt should
    request a token from and (optionally) the audience to request it for.

    Prefer an account-scoped ``audience`` (e.g. your Snowflake account URL) matching the
    Snowflake user's ``OIDC_AUDIENCE_LIST``; the bare ``snowflakecomputing.com`` default is
    shared across all Snowflake accounts and is therefore cross-account replayable.
    """

    provider: str
    audience: Optional[str] = None


def mint_workload_identity_token(config: WorkloadIdentityTokenConfig) -> str:
    """Mint a fresh token for the configured provider. Fails closed."""
    minter = _PROVIDERS.get(config.provider)
    if minter is None:
        raise DbtConfigError(
            f"Unknown workload identity token provider `{config.provider}`. "
            f"Supported providers: {', '.join(sorted(SUPPORTED_PROVIDERS))}."
        )
    return minter(config)


def _github_actions_provider(config: WorkloadIdentityTokenConfig) -> str:
    return _mint_github_actions_oidc_token(config.audience or GITHUB_OIDC_DEFAULT_AUDIENCE)


def _mint_github_actions_oidc_token(audience: str) -> str:
    """Request a fresh OIDC token from the GitHub Actions runtime.

    Reads only the standard GitHub Actions OIDC environment variables. The token
    value and the raw endpoint response are never logged or placed in error
    messages, since the success response body is itself the token.
    """
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not request_url or not request_token:
        raise DbtConfigError(
            "Minting a GitHub Actions OIDC token requires the "
            "ACTIONS_ID_TOKEN_REQUEST_URL and ACTIONS_ID_TOKEN_REQUEST_TOKEN "
            "environment variables. Ensure the workflow grants the "
            "`id-token: write` permission."
        )

    url = _github_oidc_url_with_audience(request_url, audience)
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"bearer {request_token}"},
            timeout=30,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        raise DbtRuntimeError("Failed to reach the GitHub Actions OIDC token endpoint.") from exc

    if response.status_code != 200:
        raise DbtRuntimeError(
            f"The GitHub Actions OIDC token endpoint returned status {response.status_code}."
        )

    try:
        value = response.json().get("value")
    except ValueError:
        # `from None`: the response body is the token on success; never echo it.
        raise DbtRuntimeError(
            "The GitHub Actions OIDC token endpoint returned a non-JSON response."
        ) from None

    if not value:
        raise DbtRuntimeError("The GitHub Actions OIDC token endpoint returned no token value.")
    return value


def _github_oidc_url_with_audience(request_url: str, audience: str) -> str:
    # urllib.parse and the HTTP client (urllib3) disagree on where the host ends when the
    # URL contains a backslash, whitespace, or control character: urlparse can read
    # "evil.com\\.actions.githubusercontent.com" as one trusted-looking host while urllib3
    # dials "evil.com". Reject such URLs outright so a tampered request URL can never smuggle
    # the bearer request token (which can itself mint identity tokens) to another host.
    if any(ch == "\\" or ch.isspace() or ord(ch) < 0x20 for ch in request_url):
        raise DbtRuntimeError("The GitHub Actions OIDC request URL contains invalid characters.")
    parsed = urllib.parse.urlparse(request_url)
    if parsed.scheme != "https":
        raise DbtRuntimeError("The GitHub Actions OIDC request URL must use HTTPS.")
    hostname = (parsed.hostname or "").lower()
    is_allowed_host = hostname == _GITHUB_OIDC_ALLOWED_HOST or hostname.endswith(
        "." + _GITHUB_OIDC_ALLOWED_HOST
    )
    has_empty_label = "" in hostname.split(".")
    if not is_allowed_host or has_empty_label:
        # Fail closed rather than send the bearer request token to an unknown host. The
        # empty-label check rejects ".actions.githubusercontent.com" / trailing-dot hosts
        # that slip past the suffix match but are not real GitHub hosts.
        raise DbtRuntimeError(
            "The GitHub Actions OIDC request URL host is not a recognized GitHub Actions host."
        )
    query = dict(urllib.parse.parse_qsl(parsed.query))
    query["audience"] = audience
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))


# provider name -> callable that mints a token from a WorkloadIdentityTokenConfig.
# Add new providers here; connection handling does not change.
_PROVIDERS: Dict[str, Callable[[WorkloadIdentityTokenConfig], str]] = {
    "github_actions": _github_actions_provider,
}

SUPPORTED_PROVIDERS = tuple(_PROVIDERS)
