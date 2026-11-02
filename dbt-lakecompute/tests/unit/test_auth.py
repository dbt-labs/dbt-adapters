import pytest

from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.lakecompute.auth import (
    OPTION_AUTH_API_KEY,
    OPTION_AUTH_FIVETRAN_API_URL,
    OPTION_AUTH_FIVETRAN_CREDENTIAL,
    OPTION_AUTH_METHOD,
    OPTION_AUTH_OKTA_CLIENT_ID,
    OPTION_AUTH_TOKEN,
    auth_options,
    validate_auth_credentials,
)
from dbt.adapters.lakecompute.credentials import LakeComputeCredentials


def make_credentials(**overrides) -> LakeComputeCredentials:
    kwargs = {"database": "db", "schema": "sch"}
    kwargs.update(overrides)
    return LakeComputeCredentials(**kwargs)  # type: ignore[arg-type]


class TestValidation:
    def test_unknown_method_is_rejected(self):
        with pytest.raises(FailedToConnectError, match="Unknown lakecompute auth method"):
            validate_auth_credentials(make_credentials(method="telepathy"))

    @pytest.mark.parametrize(
        "method,required",
        [
            ("token", "token"),
            ("api_key", "api_key"),
            ("fivetran", "fivetran_credential"),
        ],
    )
    def test_missing_required_field_names_the_field(self, method, required):
        with pytest.raises(FailedToConnectError, match=required):
            validate_auth_credentials(make_credentials(method=method))

    def test_okta_browser_needs_no_pre_shared_secret(self):
        validate_auth_credentials(make_credentials(method="okta_browser"))


class TestOptions:
    def test_token(self):
        options = auth_options(make_credentials(method="token", token="tok"))
        assert options == {OPTION_AUTH_METHOD: "token", OPTION_AUTH_TOKEN: "tok"}

    def test_api_key(self):
        options = auth_options(make_credentials(method="api_key", api_key="key"))
        assert options == {OPTION_AUTH_METHOD: "api_key", OPTION_AUTH_API_KEY: "key"}

    def test_fivetran_includes_optional_auth_url_only_when_set(self):
        without = auth_options(make_credentials(method="fivetran", fivetran_credential="cred"))
        assert OPTION_AUTH_FIVETRAN_CREDENTIAL in without
        assert OPTION_AUTH_FIVETRAN_API_URL not in without

        with_url = auth_options(
            make_credentials(
                method="fivetran",
                fivetran_credential="cred",
                fivetran_auth_url="https://auth/",
            )
        )
        assert with_url == {
            "adbc.dbt.auth.method": "fivetran",
            "adbc.dbt.auth.fivetran.credential": "cred",
            "adbc.dbt.auth.fivetran.api_url": "https://auth/",
        }

    def test_okta_sends_only_present_fields(self):
        options = auth_options(make_credentials(method="okta_browser", okta_client_id="client-1"))
        assert options[OPTION_AUTH_OKTA_CLIENT_ID] == "client-1"
        # the driver distinguishes "unset" from "set to empty", so absent fields stay absent
        assert len(options) == 2

    def test_empty_string_counts_as_unset(self):
        options = auth_options(make_credentials(method="okta_browser", okta_client_id=""))
        assert options == {OPTION_AUTH_METHOD: "okta_browser"}

    def test_every_option_key_is_namespaced(self):
        options = auth_options(make_credentials(method="token", token="tok"))
        assert all(key.startswith("adbc.dbt.auth.") for key in options)

    def test_values_are_all_strings(self):
        # ADBC options are a str -> str map
        options = auth_options(make_credentials(method="token", token="tok"))
        assert all(isinstance(value, str) for value in options.values())
