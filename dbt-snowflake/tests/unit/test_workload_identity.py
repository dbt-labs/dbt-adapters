from unittest.mock import Mock, patch

import pytest

from dbt_common.exceptions import DbtConfigError, DbtRuntimeError
import dbt.adapters.snowflake.workload_identity as wif


_GHA_URL = "https://token.actions.githubusercontent.com/req"


def _gha_response(value="minted_token", status_code=200, json_value=None):
    response = Mock()
    response.status_code = status_code
    response.json.return_value = {"value": value} if json_value is None else json_value
    return response


def _set_gha_env(monkeypatch, url=_GHA_URL, token="bearer-secret"):
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", url)
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", token)


def test_mint_dispatch_unknown_provider_fails_closed():
    config = wif.WorkloadIdentityTokenConfig(provider="random_ci")
    with pytest.raises(DbtConfigError) as excinfo:
        wif.mint_workload_identity_token(config)
    assert "Unknown workload identity token provider" in str(excinfo.value)
    assert "github_actions" in str(excinfo.value)


def test_mint_dispatch_github_actions_uses_default_audience(monkeypatch):
    _set_gha_env(monkeypatch)
    config = wif.WorkloadIdentityTokenConfig(provider="github_actions")  # no audience
    with patch.object(wif.requests, "get", return_value=_gha_response()) as get:
        wif.mint_workload_identity_token(config)
    assert f"audience={wif.GITHUB_OIDC_DEFAULT_AUDIENCE}" in get.call_args[0][0]


def test_github_oidc_requires_env_vars(monkeypatch):
    monkeypatch.delenv("ACTIONS_ID_TOKEN_REQUEST_URL", raising=False)
    monkeypatch.delenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", raising=False)
    with pytest.raises(DbtConfigError) as excinfo:
        wif._mint_github_actions_oidc_token("snowflakecomputing.com")
    assert "id-token: write" in str(excinfo.value)


def test_github_oidc_returns_token_value_and_hardened_request(monkeypatch):
    _set_gha_env(monkeypatch)
    with patch.object(wif.requests, "get", return_value=_gha_response("the-token")) as get:
        token = wif._mint_github_actions_oidc_token("snowflakecomputing.com")
    assert token == "the-token"
    _, kwargs = get.call_args
    assert kwargs["headers"]["Authorization"] == "bearer bearer-secret"
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] == 30


def test_github_oidc_adds_audience_query_param(monkeypatch):
    _set_gha_env(monkeypatch)
    with patch.object(wif.requests, "get", return_value=_gha_response()) as get:
        wif._mint_github_actions_oidc_token("snowflakecomputing.com")
    assert "audience=snowflakecomputing.com" in get.call_args[0][0]


def test_github_oidc_replaces_existing_audience():
    url = wif._github_oidc_url_with_audience(
        "https://token.actions.githubusercontent.com/req?audience=old&api-version=2.0",
        "snowflakecomputing.com",
    )
    assert "audience=snowflakecomputing.com" in url
    assert "audience=old" not in url
    assert "api-version=2.0" in url


def test_github_oidc_missing_value_errors(monkeypatch):
    _set_gha_env(monkeypatch)
    with patch.object(wif.requests, "get", return_value=_gha_response(json_value={})):
        with pytest.raises(DbtRuntimeError) as excinfo:
            wif._mint_github_actions_oidc_token("snowflakecomputing.com")
    assert "no token value" in str(excinfo.value)


def test_github_oidc_non_json_errors(monkeypatch):
    _set_gha_env(monkeypatch)
    response = Mock()
    response.status_code = 200
    response.json.side_effect = ValueError("not json")
    with patch.object(wif.requests, "get", return_value=response):
        with pytest.raises(DbtRuntimeError) as excinfo:
            wif._mint_github_actions_oidc_token("snowflakecomputing.com")
    assert "non-JSON" in str(excinfo.value)


def test_github_oidc_non_200_does_not_leak_response_body(monkeypatch):
    _set_gha_env(monkeypatch)
    leaky = Mock()
    leaky.status_code = 403
    leaky.text = "SUPER_SECRET_TOKEN_BODY"
    leaky.json.return_value = {"value": "SUPER_SECRET_TOKEN_BODY"}
    with patch.object(wif.requests, "get", return_value=leaky):
        with pytest.raises(DbtRuntimeError) as excinfo:
            wif._mint_github_actions_oidc_token("snowflakecomputing.com")
    message = str(excinfo.value)
    assert "403" in message
    assert "SUPER_SECRET_TOKEN_BODY" not in message


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://token.actions.githubusercontent.com/req",  # not https
        "https://evil.example.com/req",  # wrong host
        "https://token.actions.githubusercontent.com@evil.example.com/req",  # userinfo trick
    ],
)
def test_github_oidc_url_rejects_untrusted_targets(bad_url):
    with pytest.raises(DbtRuntimeError):
        wif._github_oidc_url_with_audience(bad_url, "snowflakecomputing.com")
