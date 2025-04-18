import os


ICEBERG_REST_CATALOG = """
catalogs:
-   name: my_catalogs
    read_integrations:
    -   name: my_iceberg_rest_catalog
		adapter_type: snowflake
        profile: snowflake_secondary
		catalog_type: iceberg_rest
"""


ICEBERG_REST_MODEL = """
config(
    materialized='table',
    catalog_name='my_iceberg_rest_catalog',
    catalog_table_name='my_iceberg_rest_table',
)
"""


AWS_GLUE_CATALOG = """
catalogs:
-   name: my_catalogs
    read_integrations:
    -   name: my_aws_glue_catalog
		adapter_type: snowflake
        profile: snowflake_secondary
		catalog_type: aws_glue
"""


AWS_GLUE_MODEL = """
config(
    materialized='table',
    catalog_name='my_aws_glue_catalog',
    catalog_table_name='my_aws_glue_table',
)
"""


SECONDARY_PROFILE = f"""
secondary_profiles:
-   snowflake_secondary:
	outputs:
	dev:
        type: snowflake
        threads: 4
        account: {os.getenv("SNOWFLAKE_TEST_ACCOUNT")}
        user: {os.getenv("SNOWFLAKE_TEST_USER")},
        password: {os.getenv("SNOWFLAKE_TEST_PASSWORD")},
        database: {os.getenv("SNOWFLAKE_TEST_DATABASE")},
        warehouse: {os.getenv("SNOWFLAKE_TEST_WAREHOUSE")},
	target: dev
"""
