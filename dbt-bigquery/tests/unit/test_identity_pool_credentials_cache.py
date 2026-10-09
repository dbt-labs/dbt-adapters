import threading
import time
from unittest import mock

import pytest

from dbt.adapters.bigquery import credentials as bq_credentials
from dbt.adapters.bigquery.credentials import (
    BigQueryConnectionMethod,
    BigQueryCredentials,
    create_google_credentials,
)


_IMPERSONATION_URL = (
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
    "sa@project.iam.gserviceaccount.com:generateAccessToken"
)


def _wif_credentials(request_data="grant_type=client_credentials", **kwargs):
    return BigQueryCredentials(
        method=BigQueryConnectionMethod.EXTERNAL_OAUTH_WIF,
        database="project",
        schema="dataset",
        workload_pool_provider_path="//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/pool/providers/provider",
        token_endpoint={
            "type": "entra",
            "request_url": "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
            "request_data": request_data,
        },
        **kwargs,
    )


@pytest.fixture(autouse=True)
def clear_cache():
    bq_credentials._IDENTITY_POOL_CREDENTIALS.clear()
    yield
    bq_credentials._IDENTITY_POOL_CREDENTIALS.clear()


@pytest.fixture
def token_calls():
    calls = {"idp": 0, "sts": 0}

    def fetch_subject_token(self):
        calls["idp"] += 1
        return "subject-token"

    def exchange_token(*args, **kwargs):
        calls["sts"] += 1
        time.sleep(0.2)  # so concurrent refreshes overlap
        return {"access_token": f"gcp-token-{calls['sts']}", "expires_in": 3600}

    with (
        mock.patch(
            "dbt.adapters.bigquery.token_suppliers.EntraTokenSupplier._fetch_new_token",
            fetch_subject_token,
        ),
        mock.patch("google.oauth2.sts.Client.exchange_token", side_effect=exchange_token),
    ):
        yield calls


def test_same_profile_shares_credentials():
    assert create_google_credentials(_wif_credentials()) is create_google_credentials(
        _wif_credentials()
    )


@pytest.mark.parametrize(
    "changed",
    [
        {"request_data": "grant_type=client_credentials&client_id=other"},
        {"service_account_impersonation_url": _IMPERSONATION_URL},
        {"scopes": ("https://www.googleapis.com/auth/bigquery",)},
    ],
)
def test_different_config_does_not_share_credentials(changed):
    assert create_google_credentials(_wif_credentials()) is not create_google_credentials(
        _wif_credentials(**changed)
    )


def test_concurrent_connections_fetch_token_once(token_calls):
    def connect():
        create_google_credentials(_wif_credentials()).before_request(mock.Mock(), "GET", "url", {})

    threads = [threading.Thread(target=connect) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    creds = create_google_credentials(_wif_credentials())
    assert token_calls == {"idp": 1, "sts": 1}
    assert creds.token == "gcp-token-1"


def test_refresh_with_impersonation_does_not_deadlock(token_calls):
    creds = create_google_credentials(
        _wif_credentials(service_account_impersonation_url=_IMPERSONATION_URL)
    )
    iam_response = mock.Mock(
        status=200, data=b'{"accessToken": "sa-token", "expireTime": "2099-01-01T00:00:00Z"}'
    )
    request = mock.Mock(return_value=iam_response)

    # a fresh lock of the same kind, so a deadlock fails this test instead of hanging later ones
    cls = bq_credentials._SharedIdentityPoolCredentials
    with mock.patch.object(cls, "_refresh_lock", type(cls._refresh_lock)()):
        thread = threading.Thread(target=creds.refresh, args=(request,), daemon=True)
        thread.start()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert creds.token == "sa-token"


def test_forced_refresh_of_valid_token_still_refreshes(token_calls):
    creds = create_google_credentials(_wif_credentials())
    creds.refresh(mock.Mock())
    creds.refresh(mock.Mock())

    assert token_calls["sts"] == 2
