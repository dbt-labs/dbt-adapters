import pytest

from dbt_common.exceptions import DbtConfigError

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
        # userinfo is passed through, requests reads it as basic auth
        ("dbt:s3cr3t@bq-proxy.example.com", "https://dbt:s3cr3t@bq-proxy.example.com"),
        # ipv6 literals keep their brackets
        ("https://[::1]:9050", "https://[::1]:9050"),
        ("[::1]:9050", "https://[::1]:9050"),
        # no endpoint configured, fall back to the client default
        (None, None),
        ("", None),
        ("   ", None),
    ],
)
def test_bigquery_endpoint(api_endpoint, expected):
    assert _bigquery_endpoint(api_endpoint) == expected


@pytest.mark.parametrize(
    "api_endpoint",
    [
        "https://",
        "https://bq-proxy\n.example.com",
        "https://bq-proxy .example.com",
        "https://ftp://bq-proxy.example.com",
        "ftp://bq-proxy.example.com",
        "https:/bq-proxy.example.com",
        "HTTPS:/bq-proxy.example.com",
        "https://bq-proxy.example.com:notaport",
        "https://bq-proxy.example.com:-1",
        "https://bq-proxy.example.com?dataset=x",
        "https://[::1",
    ],
)
def test_bigquery_endpoint_rejects_unparseable(api_endpoint):
    # falling back to the public bigquery.googleapis.com would silently send queries
    # somewhere other than the endpoint the user configured
    with pytest.raises(DbtConfigError):
        _bigquery_endpoint(api_endpoint)


def test_bigquery_endpoint_error_omits_the_endpoint():
    # an endpoint that failed to parse may still carry a password
    with pytest.raises(DbtConfigError) as e:
        _bigquery_endpoint("https://dbt:s3cr3t@bq-proxy .example.com")

    assert "s3cr3t" not in str(e.value)
