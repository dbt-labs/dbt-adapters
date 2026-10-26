import pytest

from dbt.adapters.lakecompute.credentials import (
    BASE_URL_ENV_VAR,
    DEFAULT_BASE_URL,
    LakeComputeCredentials,
)


def make_credentials(**overrides) -> LakeComputeCredentials:
    kwargs = {"database": "db", "schema": "sch"}
    kwargs.update(overrides)
    return LakeComputeCredentials(**kwargs)  # type: ignore[arg-type]


def test_type_is_lakecompute():
    assert make_credentials().type == "lakecompute"


def test_defaults():
    credentials = make_credentials()
    assert credentials.method == "token"
    assert credentials.retries == 1
    # off unless the profile opts in
    assert credentials.query_comment_enabled is False


def test_dbname_alias():
    credentials = LakeComputeCredentials.from_dict({"dbname": "aliased", "schema": "sch"})
    assert credentials.database == "aliased"


class TestBaseUrlResolution:
    def test_profile_wins(self, monkeypatch):
        monkeypatch.setenv(BASE_URL_ENV_VAR, "https://from-env/")
        credentials = make_credentials(base_url="https://from-profile/")
        assert credentials.resolved_base_url == "https://from-profile/"

    def test_env_var_is_second(self, monkeypatch):
        monkeypatch.setenv(BASE_URL_ENV_VAR, "https://from-env/")
        assert make_credentials().resolved_base_url == "https://from-env/"

    def test_default_is_last(self, monkeypatch):
        monkeypatch.delenv(BASE_URL_ENV_VAR, raising=False)
        assert make_credentials().resolved_base_url == DEFAULT_BASE_URL


def test_unique_field_is_base_url(monkeypatch):
    monkeypatch.delenv(BASE_URL_ENV_VAR, raising=False)
    credentials = make_credentials(base_url="https://endpoint/")
    assert credentials.unique_field == "https://endpoint/"


class TestConnectionKeys:
    @pytest.mark.parametrize("secret", ["token", "api_key", "fivetran_credential"])
    def test_secrets_are_not_reported(self, secret):
        assert secret not in make_credentials()._connection_keys()

    def test_reports_the_useful_fields(self):
        keys = make_credentials()._connection_keys()
        for expected in ("base_url", "database", "schema", "method"):
            assert expected in keys

    def test_connection_info_omits_secrets(self):
        credentials = make_credentials(token="super-secret")
        rendered = dict(credentials.connection_info())
        assert "super-secret" not in str(rendered)
