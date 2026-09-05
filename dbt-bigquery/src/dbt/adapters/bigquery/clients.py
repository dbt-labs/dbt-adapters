from __future__ import annotations

from typing import Optional, TYPE_CHECKING
from urllib.parse import urlsplit

# Lazy-loaded inside create_notebook_client() to avoid slowing down every
# `dbt parse` invocation. See: https://github.com/dbt-labs/dbt-adapters/issues/1604
if TYPE_CHECKING:
    from google.cloud import aiplatform_v1

from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.bigquery import Client as BigQueryClient, DEFAULT_RETRY as BQ_DEFAULT_RETRY
from google.cloud.dataproc_v1 import BatchControllerClient, JobControllerClient
from google.cloud.storage import Client as StorageClient
from google.cloud.storage.retry import DEFAULT_RETRY as GCS_DEFAULT_RETRY
from google.oauth2.credentials import Credentials as GoogleCredentials

from dbt_common.exceptions import DbtConfigError
from dbt.adapters.events.logging import AdapterLogger

import dbt.adapters.bigquery.__version__ as dbt_version
from dbt.adapters.bigquery.credentials import (
    BigQueryCredentials,
    create_google_credentials,
    set_default_credentials,
)


_logger = AdapterLogger("BigQuery")

_API_ENDPOINT_SCHEMES = ("http", "https")


def create_bigquery_client(credentials: BigQueryCredentials) -> BigQueryClient:
    try:
        return _create_bigquery_client(credentials)
    except DefaultCredentialsError:
        _logger.info("Please log into GCP to continue")
        set_default_credentials()
        return _create_bigquery_client(credentials)


@GCS_DEFAULT_RETRY
def create_gcs_client(credentials: BigQueryCredentials) -> StorageClient:
    return StorageClient(
        project=credentials.execution_project,
        credentials=create_google_credentials(credentials),
    )


# dataproc does not appear to have a default retry like BQ and GCS
def create_dataproc_job_controller_client(credentials: BigQueryCredentials) -> JobControllerClient:
    return JobControllerClient(
        credentials=create_google_credentials(credentials),
        client_options=ClientOptions(api_endpoint=_dataproc_endpoint(credentials)),
    )


# dataproc does not appear to have a default retry like BQ and GCS
def create_dataproc_batch_controller_client(
    credentials: BigQueryCredentials,
) -> BatchControllerClient:
    return BatchControllerClient(
        credentials=create_google_credentials(credentials),
        client_options=ClientOptions(api_endpoint=_dataproc_endpoint(credentials)),
    )


@BQ_DEFAULT_RETRY
def _create_bigquery_client(credentials: BigQueryCredentials) -> BigQueryClient:
    return BigQueryClient(
        credentials.execution_project,
        create_google_credentials(credentials),
        location=getattr(credentials, "location", None),
        client_info=ClientInfo(user_agent=f"dbt-bigquery-{dbt_version.version}"),
        client_options=ClientOptions(
            quota_project_id=credentials.quota_project,
            api_endpoint=_bigquery_endpoint(credentials.api_endpoint),
        ),
    )


def _bigquery_endpoint(api_endpoint: Optional[str]) -> Optional[str]:
    """Normalize a user-supplied `api_endpoint` into a scheme-qualified base URL.

    google-cloud-bigquery uses this value verbatim as the base of every REST URL it
    builds, so a bare host yields a schemeless URL and a duplicated scheme
    (`https://https://host`) resolves the literal host `https`. Accept both forms and
    raise on anything else, since returning `None` falls back to the public
    bigquery.googleapis.com. The endpoint is never logged; it may carry `user:password@`
    userinfo. See https://github.com/dbt-labs/dbt-adapters/issues/2103
    """
    if not (endpoint := (api_endpoint or "").strip()):
        return None

    # the innermost of a repeated scheme prefix is what the user meant
    scheme = "https"
    while (head := endpoint.partition("://"))[1] and head[0].lower() in _API_ENDPOINT_SCHEMES:
        scheme, endpoint = head[0].lower(), head[2]

    try:
        # the leading `//` forces the remainder to parse as a netloc; without it a bare
        # `localhost:9050` reads `localhost` as the scheme
        split = urlsplit(f"//{endpoint.lstrip('/')}")
        split.port  # noqa: B018  # a property access, but it validates the port
    except ValueError as e:
        raise DbtConfigError(f"Invalid api_endpoint: {e}") from e

    # urlsplit accepts more than the client can use: it drops newlines and tabs silently,
    # and reads an unsupported or mistyped scheme (`ftp://host`, `https:/host`) as a host
    if (
        any(map(str.isspace, endpoint))
        or "://" in endpoint
        or not split.hostname
        or split.hostname.lower() in _API_ENDPOINT_SCHEMES
        or split.query
        or split.fragment
    ):
        raise DbtConfigError("Invalid api_endpoint: could not parse a host")

    return f"{scheme}://{split.netloc}{split.path.rstrip('/')}"


def _dataproc_endpoint(credentials: BigQueryCredentials) -> str:
    return f"{credentials.compute_region}-dataproc.googleapis.com:443"


def create_notebook_client(
    credentials: GoogleCredentials, region: Optional[str]
) -> aiplatform_v1.NotebookServiceClient:
    from google.cloud import aiplatform_v1

    api_endpoint = f"{region}-aiplatform.googleapis.com"
    notebook_client = aiplatform_v1.NotebookServiceClient(
        credentials=credentials,
        client_options=ClientOptions(api_endpoint),
    )

    return notebook_client
