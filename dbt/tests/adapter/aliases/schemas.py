SCHEMA_YML = """
version: 2
models:
- name: foo_alias
  data_tests:
  - expect_value:
      field: tablename
      value: foo
- name: ref_foo_alias
  data_tests:
  - expect_value:
      field: tablename
      value: ref_foo_alias
- name: alias_in_project
  data_tests:
  - expect_value:
      field: tablename
      value: project_alias
- name: alias_in_project_with_override
  data_tests:
  - expect_value:
      field: tablename
      value: override_alias

"""


DUPE_CUSTOM_DATABASE__SCHEMA_YML = """
version: 2
models:
- name: model_a
  data_tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_b
  data_tests:
  - expect_value:
      field: tablename
      value: duped_alias

"""


DUPE_CUSTOM_SCHEMA__SCHEMA_YML = """
version: 2
models:
- name: model_a
  data_tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_b
  data_tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_c
  data_tests:
  - expect_value:
      field: tablename
      value: duped_alias

"""
