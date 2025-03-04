import base64
import binascii
from dataclasses import dataclass, field
from functools import lru_cache
import json
from typing import Any, Dict, Iterable, Optional, Tuple, Union

from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials
from google.oauth2.credentials import Credentials as GoogleCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.auth.identity_pool import Credentials as IdentityPoolCredentials
from mashumaro import pass_through

from dbt_common.clients.system import run_cmd
from dbt_common.dataclass_schema import ExtensibleDbtClassMixin, StrEnum
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.bigquery.auth_providers import create_token_service_client


_logger = AdapterLogger("BigQuery")

_TOKEN_FILE_PATH = "/tmp/access_token.json"


class Priority(StrEnum):
    Interactive = "interactive"
    Batch = "batch"


@dataclass
class DataprocBatchConfig(ExtensibleDbtClassMixin):
    def __init__(self, batch_config):
        self.batch_config = batch_config


class _BigQueryConnectionMethod(StrEnum):
    OAUTH = "oauth"
    OAUTH_SECRETS = "oauth-secrets"
    SERVICE_ACCOUNT = "service-account"
    SERVICE_ACCOUNT_JSON = "service-account-json"
    EXTERNAL_OAUTH_WIF = "external-oauth-wif"


@dataclass
class BigQueryCredentials(Credentials):
    method: _BigQueryConnectionMethod = None  # type: ignore

    # BigQuery allows an empty database / project, where it defers to the
    # environment for the project
    database: Optional[str] = None  # type:ignore
    schema: Optional[str] = None  # type:ignore
    execution_project: Optional[str] = None
    quota_project: Optional[str] = None
    location: Optional[str] = None
    priority: Optional[Priority] = None
    maximum_bytes_billed: Optional[int] = None
    impersonate_service_account: Optional[str] = None

    job_retry_deadline_seconds: Optional[int] = None
    job_retries: Optional[int] = 1
    job_creation_timeout_seconds: Optional[int] = None
    job_execution_timeout_seconds: Optional[int] = None

    # Keyfile json creds (unicode or base 64 encoded)
    keyfile: Optional[str] = None
    keyfile_json: Optional[Dict[str, Any]] = None

    # oauth-secrets
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_uri: Optional[str] = None

    # wif
    audience: Optional[str] = None
    service_account_impersonation_url: Optional[str] = None

    # token_endpoint
    #   a field that we expect to be a dictionary of values used to create
    #   access tokens from an external identity provider integrated with GCP's
    #   workload identity federation service
    token_endpoint: Optional[Dict[str, str]] = None

    dataproc_region: Optional[str] = None
    dataproc_cluster_name: Optional[str] = None
    gcs_bucket: Optional[str] = None

    dataproc_batch: Optional[DataprocBatchConfig] = field(
        metadata={
            "serialization_strategy": pass_through,
        },
        default=None,
    )

    scopes: Optional[Tuple[str, ...]] = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    )

    _ALIASES = {
        # 'legacy_name': 'current_name'
        "project": "database",
        "dataset": "schema",
        "target_project": "target_database",
        "target_dataset": "target_schema",
        "retries": "job_retries",
        "timeout_seconds": "job_execution_timeout_seconds",
    }

    def __post_init__(self):
        if self.keyfile_json and "private_key" in self.keyfile_json:
            self.keyfile_json["private_key"] = self.keyfile_json["private_key"].replace(
                "\\n", "\n"
            )
        if not self.method:
            raise DbtRuntimeError("Must specify authentication method")

        if not self.schema:
            raise DbtRuntimeError("Must specify schema")

    @property
    def type(self):
        return "bigquery"

    @property
    def unique_field(self):
        return self.database

    def _connection_keys(self):
        return (
            "method",
            "database",
            "execution_project",
            "schema",
            "location",
            "priority",
            "maximum_bytes_billed",
            "impersonate_service_account",
            "job_retry_deadline_seconds",
            "job_retries",
            "job_creation_timeout_seconds",
            "job_execution_timeout_seconds",
            "timeout_seconds",
            "client_id",
            "token_uri",
            "dataproc_region",
            "dataproc_cluster_name",
            "gcs_bucket",
            "dataproc_batch",
        )

    @classmethod
    def __pre_deserialize__(cls, d: Dict[Any, Any]) -> Dict[Any, Any]:
        # We need to inject the correct value of the database (aka project) at
        # this stage, ref
        # https://github.com/dbt-labs/dbt/pull/2908#discussion_r532927436.

        # `database` is an alias of `project` in BigQuery
        if "database" not in d:
            _, database = _create_bigquery_defaults()
            d["database"] = database
        # `execution_project` default to dataset/project
        if "execution_project" not in d:
            d["execution_project"] = d["database"]
        return d


def set_default_credentials() -> None:
    try:
        run_cmd(".", ["gcloud", "--version"])
    except OSError as e:
        _logger.debug(e)
        msg = """
        dbt requires the gcloud SDK to be installed to authenticate with BigQuery.
        Please download and install the SDK, or use a Service Account instead.

        https://cloud.google.com/sdk/
        """
        raise DbtRuntimeError(msg)

    run_cmd(".", ["gcloud", "auth", "application-default", "login"])


def create_google_credentials(credentials: BigQueryCredentials) -> GoogleCredentials:
    if credentials.impersonate_service_account:
        return _create_impersonated_credentials(credentials)
    return _create_google_credentials(credentials)


def _create_impersonated_credentials(credentials: BigQueryCredentials) -> ImpersonatedCredentials:
    if credentials.scopes and isinstance(credentials.scopes, Iterable):
        target_scopes = list(credentials.scopes)
    else:
        target_scopes = []

    return ImpersonatedCredentials(
        source_credentials=_create_google_credentials(credentials),
        target_principal=credentials.impersonate_service_account,
        target_scopes=target_scopes,
    )


def _create_google_credentials(credentials: BigQueryCredentials) -> GoogleCredentials:

    if credentials.method == _BigQueryConnectionMethod.OAUTH:
        creds, _ = _create_bigquery_defaults(scopes=credentials.scopes)

    elif credentials.method == _BigQueryConnectionMethod.SERVICE_ACCOUNT:
        creds = ServiceAccountCredentials.from_service_account_file(
            credentials.keyfile, scopes=credentials.scopes
        )

    elif credentials.method == _BigQueryConnectionMethod.SERVICE_ACCOUNT_JSON:
        details = credentials.keyfile_json
        if _is_base64(details):  # type:ignore
            details = _base64_to_string(details)
        creds = ServiceAccountCredentials.from_service_account_info(
            details, scopes=credentials.scopes
        )

    elif credentials.method == _BigQueryConnectionMethod.OAUTH_SECRETS:
        creds = GoogleCredentials(
            token=credentials.token,
            refresh_token=credentials.refresh_token,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            token_uri=credentials.token_uri,
            scopes=credentials.scopes,
        )

    elif credentials.method == _BigQueryConnectionMethod.EXTERNAL_OAUTH_WIF:
        creds = _create_identity_pool_credentials(credentials=credentials)

    else:
        raise FailedToConnectError(f"Invalid `method` in profile: '{credentials.method}'")

    return creds


def _create_identity_pool_credentials(credentials: BigQueryCredentials) -> GoogleCredentials:
    """
    The ADC dict here represents a configuration for a Google Cloud credential that is used to authenticate with a Google Cloud service.
    `credential_source` can either be a file path or a URL. In our case, it needs to be a file path to the access token file.
    """
    _fetch_and_save_access_token(credentials.token_endpoint)

    adc_dict = {
        "universe_domain": "googleapis.com",
        "type": "external_account",
        "audience": credentials.audience,
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
            "file": _TOKEN_FILE_PATH,
            "format": {"type": "json", "subject_token_field_name": "access_token"},
        },
    }

    if credentials.service_account_impersonation_url:
        adc_dict["service_account_impersonation_url"] = (
            credentials.service_account_impersonation_url
        )

    creds = IdentityPoolCredentials.from_info(adc_dict)
    return creds.with_scopes(credentials.scopes)


def _fetch_and_save_access_token(token_endpoint: Optional[Dict[str, Any]]) -> None:
    """
    We fetch the access token from the identity provider and save it to a file.
    We specify this file path when creating the Application Default Credentials (ADC) config.
    """
    if not token_endpoint:
        raise FailedToConnectError("token_endpoint is required for external-oauth-wif")

    token_service = create_token_service_client(token_endpoint)
    response = token_service.handle_request()
    try:
        token_data = {"access_token": response.json()["access_token"]}
    except KeyError:
        raise FailedToConnectError(
            "access_token missing from Idp token request. Please confirm correct configuration of the token_endpoint field in profiles.yml and that your Idp can obtain an OIDC-compliant access token."
        )

    try:
        with open(_TOKEN_FILE_PATH, "w") as token_file:
            json.dump(token_data, token_file)
    except (FileNotFoundError, PermissionError) as e:
        raise FailedToConnectError(
            f"Failed to write access token to {_TOKEN_FILE_PATH}. This may be due to insufficient permissions or a missing directory. Error: {str(e)}"
        )


@lru_cache()
def _create_bigquery_defaults(scopes=None) -> Tuple[Any, Optional[str]]:
    """
    Returns (credentials, project_id)

    project_id is returned available from the environment; otherwise None
    """
    # Cached, because the underlying implementation shells out, taking ~1s
    try:
        return default(scopes=scopes)
    except DefaultCredentialsError as e:
        raise DbtConfigError(f"Failed to authenticate with supplied credentials\nerror:\n{e}")


def _is_base64(s: Union[str, bytes]) -> bool:
    """
    Checks if the given string or bytes object is valid Base64 encoded.

    Args:
        s: The string or bytes object to check.

    Returns:
        True if the input is valid Base64, False otherwise.
    """

    if isinstance(s, str):
        # For strings, ensure they consist only of valid Base64 characters
        if not s.isascii():
            return False
        # Convert to bytes for decoding
        s = s.encode("ascii")

    try:
        # Use the 'validate' parameter to enforce strict Base64 decoding rules
        base64.b64decode(s, validate=True)
        return True
    except (TypeError, binascii.Error):
        return False


def _base64_to_string(b):
    return base64.b64decode(b).decode("utf-8")
