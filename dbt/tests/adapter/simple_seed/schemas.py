schema_yml = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    data_tests:
    - column_type:
        type: date
  - name: seed_id
    data_tests:
    - column_type:
        type: text

- name: seed_tricky
  columns:
  - name: seed_id
    data_tests:
    - column_type:
        type: integer
  - name: seed_id_str
    data_tests:
    - column_type:
        type: text
  - name: a_bool
    data_tests:
    - column_type:
        type: boolean
  - name: looks_like_a_bool
    data_tests:
    - column_type:
        type: text
  - name: a_date
    data_tests:
    - column_type:
        type: timestamp without time zone
  - name: looks_like_a_date
    data_tests:
    - column_type:
        type: text
  - name: relative
    data_tests:
    - column_type:
        type: text
  - name: weekday
    data_tests:
    - column_type:
        type: text
"""
