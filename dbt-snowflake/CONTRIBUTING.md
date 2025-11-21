# Contributing to `dbt-snowflake`

This document covers the incremental content beyond what is contained in the repository's [CONTRIBUTING.md](/CONTRIBUTING.md).
You are strongly encouraged to start there first if you are reading this for the first time.

## Integration Tests

Integration tests require setting up a Snowflake database connection to run locally. This varies by developer, but here's a general overview of how to set them up.

### Prerequisites

- A Snowflake account (you can create a free trial at https://www.snowflake.com/try)
- `pip` package manager installed
- The dbt-snowflake adapter installed: `pip install dbt-snowflake`

### Setting up a Snowflake test database

1. **Create a Snowflake account and warehouse:**
   - Sign up for a free Snowflake trial
   - Create a warehouse (e.g., `COMPUTE_WH`)
   - Create a database (e.g., `DBT_TEST`)

2. **Create a Snowflake user for testing:**
   ```sql
   CREATE USER dbt_test PASSWORD = 'your_secure_password';
   GRANT ROLE SYSADMIN TO USER dbt_test;
   ```

### Setting up your environment

1. **Create environment variables with your Snowflake credentials:**
   ```bash
   export SNOWFLAKE_TEST_ACCOUNT='your_account_identifier'
   export SNOWFLAKE_TEST_USER='dbt_test'
   export SNOWFLAKE_TEST_PASSWORD='your_secure_password'
   export SNOWFLAKE_TEST_DATABASE='DBT_TEST'
   export SNOWFLAKE_TEST_WAREHOUSE='COMPUTE_WH'
   export SNOWFLAKE_TEST_SCHEMA='PUBLIC'
   export SNOWFLAKE_TEST_THREADS=4
   ```

2. **Or create a `profiles.yml` file in `~/.dbt/`:**
   ```yaml
   dbt-snowflake-dev:
     outputs:
       dev:
         type: snowflake
         account: "{{ env_var('SNOWFLAKE_TEST_ACCOUNT') }}"
         user: "{{ env_var('SNOWFLAKE_TEST_USER') }}"
         password: "{{ env_var('SNOWFLAKE_TEST_PASSWORD') }}"
         database: "{{ env_var('SNOWFLAKE_TEST_DATABASE') }}"
         warehouse: "{{ env_var('SNOWFLAKE_TEST_WAREHOUSE') }}"
         schema: "{{ env_var('SNOWFLAKE_TEST_SCHEMA') }}"
         threads: "{{ env_var('SNOWFLAKE_TEST_THREADS', 4) | as_number }}"
     target: dev
   ```

### Running integration tests

1. **Install test dependencies:**
   ```bash
   pip install -r requirements-dev.txt
   ```

2. **Run the full test suite:**
   ```bash
   pytest tests/
   ```

3. **Run tests for a specific adapter feature:**
   ```bash
   pytest tests/functional/adapter/test_snowflake_specific_feature.py
   ```

4. **Run unit tests only:**
   ```bash
   pytest tests/unit/
   ```

### Test database cleanup

After running integration tests, you may want to clean up temporary databases created during testing:

```bash
pytest tests/ --cleanup-all
```

### Common issues

- **Connection errors:** Ensure your Snowflake account identifier, user, and password are correct
- **Insufficient privileges:** Make sure your test user has the `SYSADMIN` role or appropriate permissions
- **Warehouse not running:** Ensure your warehouse is active before running tests

### Additional resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [dbt-snowflake Documentation](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup)
- [dbt Testing Guide](https://docs.getdbt.com/reference/dbt-commands#test)
