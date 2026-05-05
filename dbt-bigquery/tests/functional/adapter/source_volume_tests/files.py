SCHEMA_YML = """
sources:
  - name: volume_source
    schema: "{{ target.schema }}"
    tables:
      - name: volume_test_table
"""

WILDCARD_SCHEMA_YML = """
sources:
  - name: volume_source
    schema: "{{ target.schema }}"
    tables:
      - name: events_20240101
      - name: events_20240102
"""

PARTITION_SCHEMA_YML = """
sources:
  - name: volume_source
    schema: "{{ target.schema }}"
    tables:
      - name: partitioned_volume_table
"""
