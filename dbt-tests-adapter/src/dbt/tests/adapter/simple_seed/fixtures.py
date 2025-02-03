#
# Macros
#

macros__schema_test = """
{% test column_type(model, column_name, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set validation_message = 'Got a column type of ' ~ col_types.get(column_name) ~ ', expected ' ~ type %}

    {% set val = 0 if col_types.get(column_name) == type else 1 %}
    {% if val == 1 and execute %}
        {{ log(validation_message, info=True) }}
    {% endif %}

    select '{{ validation_message }}' as validation_error
    from (select true) as nothing
    where {{ val }} = 1

{% endtest %}

"""

#
# Models
#

models__downstream_from_seed_actual = """
select * from {{ ref('seed_actual') }}

"""
models__downstream_from_seed_pipe_separated = """
select * from {{ ref('seed_pipe_separated') }}

"""
models__from_basic_seed = """
select * from {{ this.schema }}.seed_expected

"""

#
# Properties
#

properties__schema_yml = """
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
