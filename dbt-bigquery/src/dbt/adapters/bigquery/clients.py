from __future__ import annotations

import re
from typing import Optional, TYPE_CHECKING

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

from dbt.adapters.events.logging import AdapterLogger

import dbt.adapters.bigquery.__version__ as dbt_version
from dbt.adapters.bigquery.credentials import (
    BigQueryCredentials,
    create_google_credentials,
    set_default_credentials,
)


_logger = AdapterLogger("BigQuery")

# splits an endpoint into scheme and host. the scheme is optional and may repeat, in which
# case the group holds the last (innermost) one, i.e. the scheme the user actually meant;
# slashes around the host are dropped so they don't double up against the client's path.
# the host is deliberately \S rather than . so that embedded whitespace fails the match
# outright instead of reaching the client -- keep it that way
_API_ENDPOINT = re.compile(r"(?:(?P<scheme>https?)://)*/*(?P<host>\S*?)/*", re.IGNORECASE)


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
    builds, so a bare host yields a schemeless URL and a value that picked up an extra
    scheme somewhere upstream (`https://https://host`) resolves the literal host `https`.
    Accept both forms instead. See https://github.com/dbt-labs/dbt-adapters/issues/2103
    """
    if not (endpoint := (api_endpoint or "").strip()):
        return None

    match = _API_ENDPOINT.fullmatch(endpoint)
    if not match or not (host := match["host"]):
        _logger.warning(f"Ignoring api_endpoint {api_endpoint!r}: could not parse a host")
        return None

    # a scheme left over in the host means the value is malformed in a way the pattern
    # can't read as a repeated prefix: an unsupported scheme (`ftp://host`) or a mistyped
    # separator (`https:/host`, which would otherwise resolve the literal host `https`)
    if "://" in host or host.lower().startswith(("http:", "https:")):
        _logger.warning(f"Ignoring api_endpoint {api_endpoint!r}: {host!r} is not a usable host")
        return None

    normalized = f"{(match['scheme'] or 'https').lower()}://{host}"
    _logger.debug(f"Using api_endpoint {normalized}")
    return normalized


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
