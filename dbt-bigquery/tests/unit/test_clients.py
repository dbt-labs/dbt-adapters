import pytest

from dbt.adapters.bigquery.clients import _bigquery_endpoint


@pytest.mark.parametrize(
    "api_endpoint,expected",
    [
        # already well formed, left alone
        ("https://bq-proxy.example.com", "https://bq-proxy.example.com"),
        ("https://bq-proxy.example.com:3001", "https://bq-proxy.example.com:3001"),
        ("http://localhost:9050", "http://localhost:9050"),
        ("https://bq-proxy.example.com/prefix", "https://bq-proxy.example.com/prefix"),
        # bare host, scheme is supplied
        ("bq-proxy.example.com", "https://bq-proxy.example.com"),
        ("localhost:9050", "https://localhost:9050"),
        # duplicated scheme, the innermost one wins
        ("https://https://bq-proxy.example.com", "https://bq-proxy.example.com"),
        ("https://http://bq-proxy.example.com", "http://bq-proxy.example.com"),
        ("https://https://https://bq-proxy.example.com", "https://bq-proxy.example.com"),
        # cosmetic cleanup
        ("HTTPS://bq-proxy.example.com", "https://bq-proxy.example.com"),
        ("https://bq-proxy.example.com/", "https://bq-proxy.example.com"),
        (" https://bq-proxy.example.com \n", "https://bq-proxy.example.com"),
        # nothing usable, fall back to the client default
        (None, None),
        ("", None),
        ("   ", None),
        ("https://", None),
        ("https://bq-proxy\n.example.com", None),
    ],
)
def test_bigquery_endpoint(api_endpoint, expected):
    assert _bigquery_endpoint(api_endpoint) == expected
