{%- macro snowflake__get_semantic_view_rename_sql(relation, new_name) -%}
    /*
    Rename or move a semantic view to the new name.
    Args:
        relation: SnowflakeRelation - relation to be renamed
        new_name: Union[str, SnowflakeRelation] - new name for `relation`
    Returns: templated string
    */
    alter semantic view if exists {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
