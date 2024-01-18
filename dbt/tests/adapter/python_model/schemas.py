schema_yml = """version: 2
models:
  - name: my_versioned_sql_model
    versions:
      - v: 1

sources:
  - name: test_source
    loader: custom
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: test_table
        identifier: source
"""
