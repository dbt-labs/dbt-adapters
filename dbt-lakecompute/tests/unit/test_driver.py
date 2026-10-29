import pytest

from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.lakecompute.credentials import LakeComputeCredentials
from dbt.adapters.lakecompute import driver as driver_module
from dbt.adapters.lakecompute.driver import ENTRYPOINT, connection_options, resolve_driver
from dbt.adapters.lakecompute.install import DriverInstallError


def make_credentials(**overrides) -> LakeComputeCredentials:
    kwargs = {"database": "db", "schema": "sch"}
    kwargs.update(overrides)
    return LakeComputeCredentials(**kwargs)  # type: ignore[arg-type]


class TestResolveDriver:
    def test_falls_back_to_the_cdn_download(self, monkeypatch):
        calls = []

        def fake_ensure_driver():
            calls.append(True)
            return "/cache/com.getdbt/adbc/libadbc_driver_dbt-0.4.0.dylib"

        monkeypatch.setattr(driver_module, "ensure_driver", fake_ensure_driver)
        resolved = resolve_driver(make_credentials(token="tok"))

        assert calls == [True]
        assert resolved.path == "/cache/com.getdbt/adbc/libadbc_driver_dbt-0.4.0.dylib"

    def test_an_explicit_path_skips_the_download(self, monkeypatch):
        def fail() -> str:
            raise AssertionError("driver_path should bypass the download entirely")

        monkeypatch.setattr(driver_module, "ensure_driver", fail)
        resolved = resolve_driver(make_credentials(driver_path="/opt/lib/x.so", token="tok"))
        assert resolved.path == "/opt/lib/x.so"

    def test_a_failed_install_surfaces_as_a_connection_error(self, monkeypatch):
        def fail() -> str:
            raise DriverInstallError("no pinned checksum for sparc64")

        monkeypatch.setattr(driver_module, "ensure_driver", fail)
        with pytest.raises(FailedToConnectError, match="no pinned checksum"):
            resolve_driver(make_credentials(token="tok"))

    def test_missing_auth_is_reported_before_the_driver_is_fetched(self, monkeypatch):
        # a missing token should not cost a 60s download first
        def fail() -> str:
            raise AssertionError("auth must be validated before the download")

        monkeypatch.setattr(driver_module, "ensure_driver", fail)
        with pytest.raises(FailedToConnectError, match="requires `token`"):
            resolve_driver(make_credentials())

    def test_database_options(self):
        resolved = resolve_driver(
            make_credentials(
                driver_path="/opt/lib/libadbc_driver_dbt.so",
                token="tok",
                base_url="https://endpoint/",
            )
        )
        assert resolved.path == "/opt/lib/libadbc_driver_dbt.so"
        assert resolved.entrypoint == ENTRYPOINT
        assert resolved.db_kwargs["uri"] == "https://endpoint/"
        assert resolved.db_kwargs["adbc.dbt.auth.token"] == "tok"


class TestConnectionOptions:
    def test_omits_unset_fields(self):
        assert connection_options(make_credentials()) == {}

    def test_passes_through_what_is_set(self):
        options = connection_options(make_credentials(warehouse="wh", timeout_seconds=30))
        assert options == {
            "adbc.dbt.warehouse": "wh",
            "adbc.dbt.timeout_seconds": "30",
        }
