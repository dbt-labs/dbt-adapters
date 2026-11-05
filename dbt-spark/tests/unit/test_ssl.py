"""Unit tests for TLS verification in the Spark Thrift SSL transport."""

import ssl
from unittest import mock

import pytest

from dbt.adapters.spark.connections import (
    CA_BUNDLE_VARS,
    _resolve_ca_bundle,
    build_ssl_transport,
)


@pytest.fixture(autouse=True)
def clear_ca_bundle_env(monkeypatch):
    """Keep the developer's environment from influencing these tests."""
    for name in CA_BUNDLE_VARS:
        monkeypatch.delenv(name, raising=False)


def _build_ssl_context():
    """Run build_ssl_transport and return the context handed to TSSLSocket."""
    captured = {}

    def fake_ssl_socket(host, port, ssl_context=None):
        captured["ssl_context"] = ssl_context
        return mock.MagicMock()

    with mock.patch("dbt.adapters.spark.connections.TSSLSocket", side_effect=fake_ssl_socket):
        build_ssl_transport(
            host="localhost",
            port=10000,
            username="dbt",
            auth="NOSASL",
            kerberos_service_name=None,
        )

    return captured["ssl_context"]


def test_no_ca_bundle_keeps_verification_disabled():
    context = _build_ssl_context()

    assert context.check_hostname is False
    assert context.verify_mode == ssl.CERT_NONE


@pytest.mark.parametrize("env_var", CA_BUNDLE_VARS)
def test_ca_bundle_enables_verification(env_var, tmp_path, monkeypatch):
    bundle = tmp_path / "ca.pem"
    bundle.write_text("test CA bundle")
    monkeypatch.setenv(env_var, str(bundle))

    fake_context = mock.MagicMock()
    with mock.patch("ssl.create_default_context", return_value=fake_context):
        context = _build_ssl_context()

    fake_context.load_verify_locations.assert_called_once_with(cafile=str(bundle))
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_ssl_cert_file_takes_precedence_over_requests_ca_bundle(tmp_path, monkeypatch):
    ssl_cert_file = tmp_path / "ssl-cert-file.pem"
    ssl_cert_file.write_text("SSL_CERT_FILE bundle")
    requests_bundle = tmp_path / "requests-bundle.pem"
    requests_bundle.write_text("REQUESTS_CA_BUNDLE bundle")
    monkeypatch.setenv("SSL_CERT_FILE", str(ssl_cert_file))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(requests_bundle))

    fake_context = mock.MagicMock()
    with mock.patch("ssl.create_default_context", return_value=fake_context):
        _build_ssl_context()

    fake_context.load_verify_locations.assert_called_once_with(cafile=str(ssl_cert_file))


def test_missing_ssl_cert_file_falls_back_to_requests_ca_bundle(tmp_path, monkeypatch):
    requests_bundle = tmp_path / "requests-bundle.pem"
    requests_bundle.write_text("REQUESTS_CA_BUNDLE bundle")
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "missing.pem"))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(requests_bundle))

    fake_context = mock.MagicMock()
    with mock.patch("ssl.create_default_context", return_value=fake_context):
        context = _build_ssl_context()

    fake_context.load_verify_locations.assert_called_once_with(cafile=str(requests_bundle))
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_unreadable_ca_bundle_keeps_verification_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "missing.pem"))

    context = _build_ssl_context()

    assert context.check_hostname is False
    assert context.verify_mode == ssl.CERT_NONE


@pytest.mark.parametrize(
    "load_error",
    [ssl.SSLError("invalid certificate"), OSError("cannot read bundle")],
)
def test_ca_bundle_load_failure_keeps_verification_disabled(load_error, tmp_path, monkeypatch):
    bundle = tmp_path / "bad-ca.pem"
    bundle.write_text("not a valid certificate")
    monkeypatch.setenv("SSL_CERT_FILE", str(bundle))

    fake_context = mock.MagicMock()
    fake_context.load_verify_locations.side_effect = load_error
    with mock.patch("ssl.create_default_context", return_value=fake_context):
        context = _build_ssl_context()

    assert context.check_hostname is False
    assert context.verify_mode == ssl.CERT_NONE


def test_resolve_ca_bundle_returns_none_without_environment_variables():
    assert _resolve_ca_bundle() is None


@pytest.mark.parametrize("env_var", CA_BUNDLE_VARS)
def test_resolve_ca_bundle_ignores_empty_environment_variables(env_var, monkeypatch):
    monkeypatch.setenv(env_var, "")

    assert _resolve_ca_bundle() is None


def test_resolve_ca_bundle_skips_missing_candidates(tmp_path, monkeypatch):
    usable_bundle = tmp_path / "usable.pem"
    usable_bundle.write_text("usable CA bundle")
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "missing.pem"))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(usable_bundle))

    assert _resolve_ca_bundle() == str(usable_bundle)
