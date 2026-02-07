{% materialization hybrid_table, adapter='snowflake' %}
    {#
        Hybrid Table Materialization for Snowflake

        Snowflake hybrid tables provide unistore workload capability, combining
        transactional (OLTP) and analytical (OLAP) processing in a single table.

        Features supported:
        - PRIMARY KEY (required)
        - UNIQUE constraints
        - FOREIGN KEY constraints (references other hybrid tables)
        - Secondary indexes with optional INCLUDE columns
        - AUTOINCREMENT/IDENTITY columns
        - DEFAULT values
        - Table COMMENT
        - Incremental MERGE operations

        See: https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table
    #}

    {# Set query tag for monitoring #}
    {% set query_tag = set_query_tag() %}

    {# Load existing relation and set target #}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.HybridTable) %}

    {{ run_hooks(pre_hooks) }}

    {# ============== CONFIGURATION ============== #}

    {# Required configuration #}
    {% set column_definitions = config.get('column_definitions', {}) %}
    {% set primary_key = config.get('primary_key', []) %}

    {# Optional configuration - constraints and indexes #}
    {% set indexes = config.get('indexes', []) %}
    {% set unique_constraints = config.get('unique_constraints', []) %}
    {% set foreign_keys = config.get('foreign_keys', []) %}

    {# Optional configuration - behavior #}
    {% set force_ctas = config.get('force_ctas', false) %}
    {% set merge_exclude_columns = config.get('merge_exclude_columns', []) %}
    {% set merge_update_columns = config.get('merge_update_columns', []) %}
    {% set full_refresh_mode = should_full_refresh() %}

    {# Optional configuration - table properties #}
    {% set table_comment = config.get('comment', none) %}

    {# Get explicit column order if provided, otherwise sort alphabetically for consistency #}
    {% set column_order = config.get('column_order', column_definitions.keys() | sort | list) %}

    {# ============== VALIDATION ============== #}

    {% if column_definitions | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "Hybrid table materialization requires 'column_definitions' in model config.\n\n" ~
            "Example:\n" ~
            "{{ config(\n" ~
            "    materialized='hybrid_table',\n" ~
            "    column_definitions={\n" ~
            "        'id': 'INT NOT NULL',\n" ~
            "        'name': 'VARCHAR(200)',\n" ~
            "        'created_at': 'TIMESTAMP_NTZ'\n" ~
            "    },\n" ~
            "    primary_key=['id']\n" ~
            ") }}"
        ) }}
    {% endif %}

    {% if primary_key | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "Hybrid tables require a PRIMARY KEY constraint.\n\n" ~
            "Add 'primary_key' to model config:\n" ~
            "  primary_key=['id']  -- single column\n" ~
            "  primary_key=['tenant_id', 'user_id']  -- composite key\n\n" ~
            "Note: PRIMARY KEY columns cannot use VARIANT, ARRAY, OBJECT,\n" ~
            "GEOGRAPHY, GEOMETRY, VECTOR, or TIMESTAMP_TZ types."
        ) }}
    {% endif %}

    {# Validate primary key columns exist in column_definitions #}
    {% for pk_col in primary_key %}
        {% if pk_col not in column_definitions %}
            {{ exceptions.raise_compiler_error(
                "Primary key column '" ~ pk_col ~ "' not found in column_definitions.\n" ~
                "Defined columns: " ~ (column_definitions.keys() | list | join(', '))
            ) }}
        {% endif %}
    {% endfor %}

    {# Validate unique constraint columns exist #}
    {% for unique in unique_constraints %}
        {% for col in unique.columns %}
            {% if col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "UNIQUE constraint column '" ~ col ~ "' not found in column_definitions."
                ) }}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {# Validate index columns exist #}
    {% for index in indexes %}
        {% for col in index.columns %}
            {% if col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "Index column '" ~ col ~ "' in index '" ~ index.name ~ "' not found in column_definitions."
                ) }}
            {% endif %}
        {% endfor %}
        {# Validate INCLUDE columns if specified #}
        {% if index.include is defined %}
            {% for col in index.include %}
                {% if col not in column_definitions %}
                    {{ exceptions.raise_compiler_error(
                        "INCLUDE column '" ~ col ~ "' in index '" ~ index.name ~ "' not found in column_definitions."
                    ) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {# ============== RELATION TYPE CHANGE DETECTION ============== #}

    {% if existing_relation and not existing_relation.is_hybrid_table %}
        {{ log("Dropping " ~ existing_relation ~ " because it is not a hybrid table", info=True) }}
        {{ adapter.drop_relation(existing_relation) }}
        {% set existing_relation = none %}
    {% endif %}

    {# ============== CREATE OR MERGE ============== #}

    {% if existing_relation is none or full_refresh_mode or force_ctas %}
        {# ========== CREATE OR REPLACE HYBRID TABLE ========== #}

        {% call statement('main') %}
            CREATE OR REPLACE HYBRID TABLE {{ target_relation }} (
                {# Column definitions with types, defaults, autoincrement #}
                {% for column in column_order %}
                    {{ column }} {{ column_definitions[column] }}{% if not loop.last %},{% endif %}
                {% endfor %}

                {# PRIMARY KEY constraint (required) #}
                {% if primary_key %}
                    , PRIMARY KEY ({{ primary_key | join(', ') }})
                {% endif %}

                {# UNIQUE constraints (optional) #}
                {% for unique in unique_constraints %}
                    , {% if unique.name is defined %}CONSTRAINT {{ unique.name }} {% endif %}UNIQUE ({{ unique.columns | join(', ') }})
                {% endfor %}

                {# FOREIGN KEY constraints (optional) #}
                {% for fk in foreign_keys %}
                    , {% if fk.name is defined %}CONSTRAINT {{ fk.name }} {% endif %}FOREIGN KEY ({{ fk.columns | join(', ') }}) REFERENCES {{ fk.references.table }} ({{ fk.references.columns | join(', ') }})
                {% endfor %}

                {# Secondary indexes (optional, with INCLUDE support) #}
                {% for index in indexes %}
                    , INDEX {{ index.name }} ({{ index.columns | join(', ') }}){% if index.include is defined %} INCLUDE ({{ index.include | join(', ') }}){% endif %}
                {% endfor %}
            )
            {# Table comment (optional) #}
            {% if table_comment %}
            COMMENT = '{{ table_comment | replace("'", "''") }}'
            {% endif %}
            AS (
                {{ sql }}
            )
        {% endcall %}

        {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}

    {% else %}
        {# ========== INCREMENTAL MERGE INTO EXISTING HYBRID TABLE ========== #}

        {# Determine columns to update #}
        {% if merge_update_columns | length > 0 %}
            {% set update_columns = merge_update_columns %}
        {% else %}
            {# Exclude primary keys and explicitly excluded columns from updates #}
            {# Also exclude AUTOINCREMENT columns (they contain 'AUTOINCREMENT' or 'IDENTITY' in definition) #}
            {% set update_columns = [] %}
            {% for col in column_order %}
                {% set col_def = column_definitions[col] | upper %}
                {% if col not in primary_key
                   and col not in merge_exclude_columns
                   and 'AUTOINCREMENT' not in col_def
                   and 'IDENTITY' not in col_def %}
                    {% do update_columns.append(col) %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% call statement('main') %}
            MERGE INTO {{ target_relation }} AS target
            USING ({{ sql }}) AS source
            ON {% for pk in primary_key %}
                target.{{ pk }} = source.{{ pk }}{% if not loop.last %} AND {% endif %}
            {% endfor %}
            {% if update_columns | length > 0 %}
            WHEN MATCHED THEN
                UPDATE SET
                {% for column in update_columns %}
                    target.{{ column }} = source.{{ column }}{% if not loop.last %},{% endif %}
                {% endfor %}
            {% endif %}
            WHEN NOT MATCHED THEN
                INSERT ({{ column_order | join(', ') }})
                VALUES ({% for col in column_order %}source.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %})
        {% endcall %}

        {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=False) %}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {% do unset_query_tag(query_tag) %}

    {# Apply grants #}
    {% set grant_config = config.get('grants') %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {# Persist documentation #}
    {% do persist_docs(target_relation, model) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{# ============== HELPER MACROS FOR HYBRID TABLES ============== #}

{% macro snowflake__get_drop_hybrid_table_sql(relation) %}
    drop hybrid table if exists {{ relation }}
{% endmacro %}


{% macro snowflake__get_rename_hybrid_table_sql(relation, new_name) %}
    {#
    Rename a hybrid table.

    Args:
        relation: SnowflakeRelation - hybrid table relation to be renamed
        new_name: Union[str, SnowflakeRelation] - new name for `relation`
    Returns: SQL string
    #}
    alter hybrid table {{ relation }} rename to {{ new_name }}
{% endmacro %}
