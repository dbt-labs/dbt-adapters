SCHEMA_YML = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_source
"""

BATCH_SCHEMA_YML = """
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ target.schema }}"
    tables:
      - name: test_table
      - name: test_table2
      - name: test_table_with_loaded_at_field
        loaded_at_field: my_loaded_at_field
"""

SEED_TEST_SOURCE_CSV = """
id,name
1,Martin
2,Jeter
3,Ruth
4,Gehrig
5,DiMaggio
6,Torre
7,Mantle
8,Berra
9,Maris
""".strip()
