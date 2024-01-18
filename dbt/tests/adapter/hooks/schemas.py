seed_models = """
version: 2
seeds:
- name: example_seed
  columns:
  - name: new_col
    data_tests:
    - not_null
"""


test_snapshot_models = """
version: 2
snapshots:
- name: example_snapshot
  columns:
  - name: new_col
    data_tests:
    - not_null
"""
