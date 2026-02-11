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
    {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
    {% set incremental_strategy = (config.get('incremental_strategy') or 'merge') | lower %}
    {% set force_non_ctas = config.get('force_non_ctas', none) %}
    {% set autoincrement_columns = [] %}
    {% for column_name, column_definition in column_definitions.items() %}
        {% set column_definition_upper = column_definition | upper %}
        {% if 'AUTOINCREMENT' in column_definition_upper or 'IDENTITY' in column_definition_upper %}
            {% do autoincrement_columns.append(column_name) %}
        {% endif %}
    {% endfor %}

    {% if force_non_ctas is not none %}
        {% set use_non_ctas_create = force_non_ctas %}
    {% else %}
        {# Force non-CTAS when AUTOINCREMENT or FK are present (Snowflake requires FK at CREATE time) #}
        {% set use_non_ctas_create = autoincrement_columns | length > 0 or foreign_keys | length > 0 %}
    {% endif %}

    {# Optional configuration - table properties #}
    {% set table_comment = config.get('comment', none) %}

    {# Get explicit column order if provided, otherwise sort alphabetically for consistency #}
    {% set column_order = config.get('column_order', column_definitions.keys() | sort | list) %}

    {% set supported_incremental_strategies = ['merge', 'delete+insert'] %}
    {% if incremental_strategy not in supported_incremental_strategies %}
        {{ exceptions.raise_compiler_error(
            "Invalid incremental_strategy '" ~ incremental_strategy ~ "' for hybrid_table. Supported strategies: " ~
            supported_incremental_strategies | join(', ')
        ) }}
    {% endif %}

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

    {% set unsupported_constraint_types = ['VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY', 'VECTOR', 'TIMESTAMP_TZ'] %}

    {# Validate primary key columns exist in column_definitions #}
    {% for pk_col in primary_key %}
        {% if pk_col not in column_definitions %}
            {{ exceptions.raise_compiler_error(
                "Primary key column '" ~ pk_col ~ "' not found in column_definitions.\n" ~
                "Defined columns: " ~ (column_definitions.keys() | list | join(', '))
            ) }}
        {% endif %}
        {% set pk_type = column_definitions[pk_col] | upper %}
        {% for unsupported in unsupported_constraint_types %}
            {% if unsupported in pk_type %}
                {{ exceptions.raise_compiler_error(
                    "Primary key column '" ~ pk_col ~ "' uses unsupported data type '" ~ column_definitions[pk_col] ~ "'."
                ) }}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {# Validate unique constraint columns exist #}
    {% for unique in unique_constraints %}
        {% for col in unique.columns %}
            {% if col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "UNIQUE constraint column '" ~ col ~ "' not found in column_definitions."
                ) }}
            {% endif %}
            {% set unique_type = column_definitions[col] | upper %}
            {% for unsupported in unsupported_constraint_types %}
                {% if unsupported in unique_type %}
                    {{ exceptions.raise_compiler_error(
                        "UNIQUE constraint column '" ~ col ~ "' uses unsupported data type '" ~ column_definitions[col] ~ "'."
                    ) }}
                {% endif %}
            {% endfor %}
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
            {% set index_type = column_definitions[col] | upper %}
            {% for unsupported in unsupported_constraint_types %}
                {% if unsupported in index_type %}
                    {{ exceptions.raise_compiler_error(
                        "Index column '" ~ col ~ "' in index '" ~ index.name ~ "' uses unsupported data type '" ~ column_definitions[col] ~ "'."
                    ) }}
                {% endif %}
            {% endfor %}
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

    {# Validate foreign key configuration and referenced relations #}
    {% set validated_foreign_keys = [] %}
    {% for fk in foreign_keys %}
        {% if fk.columns is not defined or fk.columns | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Foreign key configuration requires 'columns' with at least one column name."
            ) }}
        {% endif %}

        {% for fk_col in fk.columns %}
            {% if fk_col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "Foreign key column '" ~ fk_col ~ "' not found in column_definitions."
                ) }}
            {% endif %}
            {% set fk_col_type = column_definitions[fk_col] | upper %}
            {% for unsupported in unsupported_constraint_types %}
                {% if unsupported in fk_col_type %}
                    {{ exceptions.raise_compiler_error(
                        "Foreign key column '" ~ fk_col ~ "' uses unsupported data type '" ~ column_definitions[fk_col] ~ "'."
                    ) }}
                {% endif %}
            {% endfor %}
        {% endfor %}

        {% if fk.references is not defined %}
            {{ exceptions.raise_compiler_error(
                "Foreign key configuration requires a 'references' dictionary with table and columns."
            ) }}
        {% endif %}

        {% set fk_reference = fk.references %}
        {% if fk_reference.columns is not defined or fk_reference.columns | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references must include 'columns' with at least one column name."
            ) }}
        {% endif %}

        {% if fk_reference.table is not defined and fk_reference.identifier is not defined %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references must include 'table' (optionally with 'database'/'schema')."
            ) }}
        {% endif %}

        {% if fk_reference.columns | length != fk.columns | length %}
            {{ exceptions.raise_compiler_error(
                "Foreign key column count mismatch. Referencing columns (" ~ (fk.columns | join(', ')) ~ ") " ~
                "must match referenced columns (" ~ (fk_reference.columns | join(', ')) ~ ")."
            ) }}
        {% endif %}

        {% set reference_database = fk_reference.database if fk_reference.database is defined else target_relation.database %}
        {% set reference_schema = fk_reference.schema if fk_reference.schema is defined else target_relation.schema %}
        {% set reference_identifier_value = fk_reference.identifier if fk_reference.identifier is defined else fk_reference.table %}

        {% if reference_identifier_value is none %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references must define a target relation via 'table' or 'identifier'."
            ) }}
        {% endif %}

        {% if reference_identifier_value.database is defined %}
            {% set reference_database = reference_identifier_value.database %}
            {% set reference_schema = reference_identifier_value.schema %}
            {% set reference_identifier = reference_identifier_value.identifier %}
        {% else %}
            {% set reference_identifier = reference_identifier_value %}
        {% endif %}

        {% if reference_identifier is string %}
            {% set identifier_parts = reference_identifier.split('.') %}
            {% if identifier_parts | length == 3 %}
                {% set reference_database = identifier_parts[0] %}
                {% set reference_schema = identifier_parts[1] %}
                {% set reference_identifier = identifier_parts[2] %}
            {% elif identifier_parts | length == 2 %}
                {% if fk_reference.schema is not defined %}
                    {% set reference_schema = identifier_parts[0] %}
                {% endif %}
                {% set reference_identifier = identifier_parts[1] %}
            {% endif %}
        {% endif %}

        {% if reference_database is none %}
            {% set reference_database = target_relation.database %}
        {% endif %}
        {% if reference_schema is none %}
            {% set reference_schema = target_relation.schema %}
        {% endif %}

        {% set referenced_relation = adapter.get_relation(
            database=reference_database,
            schema=reference_schema,
            identifier=reference_identifier
        ) %}

        {% if referenced_relation is none %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references relation '" ~ reference_database ~ "." ~ reference_schema ~ "." ~ reference_identifier ~ "' which does not exist."
            ) }}
        {% endif %}

        {% set referenced_column_names = [] %}
        {% set reference_columns = adapter.get_columns_in_relation(referenced_relation) %}
        {% for reference_column in reference_columns %}
            {% do referenced_column_names.append(reference_column.name | upper) %}
        {% endfor %}

        {% for reference_column_name in fk_reference.columns %}
            {% if reference_column_name | upper not in referenced_column_names %}
                {{ exceptions.raise_compiler_error(
                    "Foreign key references column '" ~ reference_column_name ~ "' that does not exist on " ~ referenced_relation ~ "."
                ) }}
            {% endif %}
        {% endfor %}

        {% set normalized_fk = {
            'name': fk.name if fk.name is defined else none,
            'columns': fk.columns,
            'references_relation': referenced_relation,
            'references_columns': fk_reference.columns
        } %}
        {% do validated_foreign_keys.append(normalized_fk) %}
    {% endfor %}

    {% set foreign_keys = validated_foreign_keys %}

    {# ============== RELATION TYPE CHANGE DETECTION ============== #}

    {% if existing_relation and not existing_relation.is_hybrid_table %}
        {{ log("Dropping " ~ existing_relation ~ " because it is not a hybrid table", info=True) }}
        {{ adapter.drop_relation(existing_relation) }}
        {% set existing_relation = none %}
    {% endif %}

    {# ============== CREATE OR MERGE ============== #}

    {% if existing_relation is none or full_refresh_mode or force_ctas %}
        {# ========== CREATE OR REPLACE HYBRID TABLE ========== #}

        {% if use_non_ctas_create %}
            {% if autoincrement_columns | length > 0 %}
                {{ log("Using CREATE + INSERT flow because AUTOINCREMENT/IDENTITY columns are present", info=True) }}
            {% elif foreign_keys | length > 0 %}
                {{ log("Using CREATE + INSERT flow because FOREIGN KEY constraints require DDL-time definition", info=True) }}
            {% elif force_non_ctas %}
                {{ log("Using CREATE + INSERT flow due to 'force_non_ctas' config", info=True) }}
            {% endif %}

            {% call statement('create_hybrid_table') %}
                CREATE OR REPLACE HYBRID TABLE {{ target_relation }} (
                    {% for column in column_order %}
                        {{ column }} {{ column_definitions[column] }}{% if not loop.last %},{% endif %}
                    {% endfor %}

                    {% if primary_key %}
                        , PRIMARY KEY ({{ primary_key | join(', ') }})
                    {% endif %}

                    {% for unique in unique_constraints %}
                        , {% if unique.name is defined %}CONSTRAINT {{ unique.name }} {% endif %}UNIQUE ({{ unique.columns | join(', ') }})
                    {% endfor %}

                    {% for fk in foreign_keys %}
                        , {% if fk.name is not none %}CONSTRAINT {{ fk.name }} {% endif %}FOREIGN KEY ({{ fk.columns | join(', ') }}) REFERENCES {{ fk.references_relation }} ({{ fk.references_columns | join(', ') }})
                    {% endfor %}

                    {% for index in indexes %}
                        , INDEX {{ index.name }} ({{ index.columns | join(', ') }}){% if index.include is defined %} INCLUDE ({{ index.include | join(', ') }}){% endif %}
                    {% endfor %}
                )
                {% if table_comment %}
                COMMENT = '{{ table_comment | replace("'", "''") }}'
                {% endif %}
            {% endcall %}

            {# Exclude AUTOINCREMENT columns from INSERT — Snowflake auto-generates them #}
            {% set insert_columns = [] %}
            {% for col in column_order %}
                {% if col not in autoincrement_columns %}
                    {% do insert_columns.append(col) %}
                {% endif %}
            {% endfor %}

            {% call statement('main') %}
                INSERT INTO {{ target_relation }} ({{ insert_columns | join(', ') }})
                {{ sql }}
            {% endcall %}
        {% else %}
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
        {% endif %}

        {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}

    {% else %}
        {# ========== INCREMENTAL MERGE INTO EXISTING HYBRID TABLE ========== #}

        {% if on_schema_change != 'ignore' %}
            {% set schema_changes = snowflake__get_hybrid_table_schema_changes(existing_relation, config) %}
            {{ snowflake__apply_hybrid_table_schema_changes(on_schema_change, target_relation, schema_changes) }}
        {% endif %}

        {% if incremental_strategy == 'merge' %}
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

            {# Build merge key list excluding AUTOINCREMENT PKs (not in source data) #}
            {% set merge_keys = [] %}
            {% for pk in primary_key %}
                {% if pk not in autoincrement_columns %}
                    {% do merge_keys.append(pk) %}
                {% endif %}
            {% endfor %}

            {# Build insert column list excluding AUTOINCREMENT columns #}
            {% set merge_insert_columns = [] %}
            {% for col in column_order %}
                {% if col not in autoincrement_columns %}
                    {% do merge_insert_columns.append(col) %}
                {% endif %}
            {% endfor %}

            {% if merge_keys | length == 0 %}
                {# All PKs are AUTOINCREMENT — can't MERGE, just INSERT new rows #}
                {% call statement('main') %}
                    INSERT INTO {{ target_relation }} ({{ merge_insert_columns | join(', ') }})
                    {{ sql }}
                {% endcall %}
            {% else %}
                {% call statement('main') %}
                    MERGE INTO {{ target_relation }} AS target
                    USING ({{ sql }}) AS source
                    ON {% for pk in merge_keys %}
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
                        INSERT ({{ merge_insert_columns | join(', ') }})
                        VALUES ({% for col in merge_insert_columns %}source.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %})
                {% endcall %}
            {% endif %}
        {% elif incremental_strategy == 'delete+insert' %}
            {% if primary_key | length == 0 %}
                {{ exceptions.raise_compiler_error(
                    "incremental_strategy='delete+insert' requires a primary_key configuration."
                ) }}
            {% endif %}

            {# Build non-autoincrement column lists for delete+insert #}
            {% set di_merge_keys = [] %}
            {% for pk in primary_key %}
                {% if pk not in autoincrement_columns %}
                    {% do di_merge_keys.append(pk) %}
                {% endif %}
            {% endfor %}
            {% set di_insert_columns = [] %}
            {% for col in column_order %}
                {% if col not in autoincrement_columns %}
                    {% do di_insert_columns.append(col) %}
                {% endif %}
            {% endfor %}

            {% if di_merge_keys | length == 0 %}
                {{ exceptions.raise_compiler_error(
                    "incremental_strategy='delete+insert' requires at least one non-AUTOINCREMENT primary key column."
                ) }}
            {% endif %}

            {% call statement('delete_incremental') %}
                DELETE FROM {{ target_relation }} AS target
                USING ({{ sql }}) AS source
                WHERE {% for pk in di_merge_keys %}
                    target.{{ pk }} = source.{{ pk }}{% if not loop.last %} AND {% endif %}
                {% endfor %}
            {% endcall %}

            {% call statement('main') %}
                INSERT INTO {{ target_relation }} ({{ di_insert_columns | join(', ') }})
                {{ sql }}
            {% endcall %}
        {% endif %}

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
    drop table if exists {{ relation }}
{% endmacro %}


{% macro snowflake__get_rename_hybrid_table_sql(relation, new_name) %}
    {#
    Rename a hybrid table.

    Args:
        relation: SnowflakeRelation - hybrid table relation to be renamed
        new_name: Union[str, SnowflakeRelation] - new name for `relation`
    Returns: SQL string
    #}
    alter table {{ relation }} rename to {{ new_name }}
{% endmacro %}
