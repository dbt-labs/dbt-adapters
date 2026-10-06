import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.token_suppliers import EntraTokenSupplier


class _ScriptedIdp:
    """A local identity provider that replies with a scripted sequence of status codes."""

    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.request_count = 0

        idp = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                idp.request_count += 1
                status = idp.statuses.pop(0) if len(idp.statuses) > 1 else idp.statuses[0]
                body = {"access_token": "token", "expires_in": 3600} if status == 200 else {}
                payload = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/token"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def supplier(self):
        return EntraTokenSupplier(
            {"type": "entra", "request_url": self.url, "request_data": "grant_type=client"}
        )


@pytest.fixture
def scripted_idp(mocker):
    # skip backoff sleeps so the retry tests run instantly
    mocker.patch("urllib3.util.retry.Retry.get_backoff_time", return_value=0)
    idps = []

    def _make(statuses):
        idp = _ScriptedIdp(statuses)
        idps.append(idp)
        return idp

    yield _make
    for idp in idps:
        idp.server.shutdown()
        idp.server.server_close()


@pytest.mark.parametrize("transient_status", [429, 500, 502, 503, 504])
def test_transient_idp_errors_are_retried(scripted_idp, transient_status):
    idp = scripted_idp([transient_status, transient_status, 200])

    assert idp.supplier().get_subject_token(None, None) == "token"
    assert idp.request_count == 3


def test_persistent_rate_limit_raises_after_retries(scripted_idp):
    idp = scripted_idp([429])

    with pytest.raises(DbtRuntimeError, match="Rate limit"):
        idp.supplier().get_subject_token(None, None)
    assert idp.request_count == 4  # initial attempt + 3 retries


def test_persistent_server_error_raises_after_retries(scripted_idp):
    idp = scripted_idp([503])

    with pytest.raises(requests.HTTPError):
        idp.supplier().get_subject_token(None, None)
    assert idp.request_count == 4


def test_client_errors_are_not_retried(scripted_idp):
    idp = scripted_idp([400])

    with pytest.raises(requests.HTTPError):
        idp.supplier().get_subject_token(None, None)
    assert idp.request_count == 1
