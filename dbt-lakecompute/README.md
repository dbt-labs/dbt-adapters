# dbt-lakecompute

The lake compute (dbt Compute) adapter plugin for dbt-core.

Lake compute is a remote query service whose workers run DuckDB over Iceberg tables and accept
DuckDB-dialect SQL.

The adapter is under active development. It currently implements the connection layer:
authentication, the driver download, the connection manager, and the schema metadata dbt needs
to connect and resolve relations. `dbt debug`, `dbt compile` and `dbt show` work.
Materializations, incremental models and seeds are not implemented yet, so do not use
`dbt run`, `dbt build` or `dbt seed` against this adapter.

Identifiers are never quoted, matching the service's own SQL, and are matched
case-insensitively: a relation created as `MixedCase` is found by `ref('mixedcase')` and still
renders as `MixedCase`.

## Profile

`method` defaults to `token`, which reads `token` — a service bearer token. **For a Fivetran
personal access token (a `dct_` PAT) use `method: fivetran` and `fivetran_credential` instead:**
a PAT is exchanged for a session rather than sent as a bearer token, so `method: token` with a
PAT fails with `401 invalid token`.

Always supply auth material through an environment variable, never inline.

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: lakecompute
      database: my_database
      schema: my_schema

      method: fivetran
      fivetran_credential: "{{ env_var('DBT_COMPUTE_AUTH_TOKEN') }}"

      # optional; falls back to $DBT_COMPUTE_BASE_URL, then the public endpoint
      base_url: https://api.lake-compute.fivetran.com/
```

With a bearer token, the last two auth lines become:

```yaml
      method: token   # the default, so it can be omitted
      token: "{{ env_var('DBT_COMPUTE_AUTH_TOKEN') }}"
```

Set `DBT_COMPUTE_AUTH_TOKEN` to whichever credential you are using. The ADBC driver is fetched
automatically; see [Driver](#driver) below. Run `dbt debug` to check the connection.

## Driver

The adapter needs a closed-source ADBC shared library, which is not published to PyPI. It
downloads it from dbt's public CDN the first time a connection is opened and caches it
at `{cache_dir}/com.getdbt/adbc/{arch}-{os}/`, so a machine that already has it downloads
nothing. Later runs only stat the file.

The driver version is pinned, and every platform artifact is verified against a pinned
SHA-256. A platform with no pinned checksum is refused rather than installed unverified.
Both live in [`install.py`](src/dbt/adapters/lakecompute/install.py).

| Variable | Purpose |
|---|---|
| `DBT_COMPUTE_DRIVER_CACHE` | Use this directory instead of the OS cache directory. For CI images and air-gapped installs that pre-populate the cache. |

Set `driver_path` in the profile to bypass the download entirely — for a locally built driver,
or a platform with no prebuilt artifact.

## Connection settings

| Setting | Default | Notes |
|---|---|---|
| `database` | — | Required. The catalog to connect to |
| `schema` | — | Required. The default schema |
| `method` | `token` | One of `token`, `api_key`, `okta_browser`, `fivetran`. Set explicitly to `fivetran` for a `dct_` PAT |
| `token` | — | Required with `method: token`; a service bearer token |
| `api_key` | — | Required with `method: api_key` |
| `fivetran_credential` | — | Required with `method: fivetran`; a `dct_` PAT |
| `fivetran_auth_url` | service default | Override the PAT exchange endpoint |
| `okta_auth_url`, `okta_token_url`, `okta_client_id` | service defaults | Optional with `method: okta_browser`, which needs no pre-shared secret |
| `base_url` | public endpoint | profile → `$DBT_COMPUTE_BASE_URL` → default |
| `driver_path` | — | Override the driver library path; skips the CDN download. See [Driver](#driver) |
| `warehouse`, `dialect` | — | passed through as `adbc.dbt.*` |
| `timeout_seconds` | — | passed through |
| `retries` | `1` | connection retry attempts |
| `query_comment_enabled` | `false` | dbt's query comment is off by default |

## Development

```shell
cd dbt-lakecompute
hatch run setup
hatch run code-quality
hatch run unit-tests
hatch run integration-tests
```

Unit tests need no endpoint. Never commit `test.env`.

The functional suite runs against a live endpoint. Copy `test.env.example` to `test.env`. It needs
`DBT_COMPUTE_BASE_URL`, `DBT_COMPUTE_AUTH_TOKEN` and `DBT_COMPUTE_DATABASE`; the tests skip
without them. `DBT_COMPUTE_AUTH_METHOD` selects the auth method (`fivetran` by default; set `token`
for a plain bearer token). Set `DBT_COMPUTE_SCHEMA` as well to build everything in one named
scratch schema rather than a generated name per test file — useful against a shared catalog,
and it makes leftovers easy to find if a run is interrupted. Pinning the schema means test
classes cannot run in parallel, so pass `-n0` with it. Every statement is a full round trip at
3–15 seconds, so a run of the whole functional suite takes tens of minutes.
