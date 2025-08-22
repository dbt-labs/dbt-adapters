{% macro postgres__get_relations() -%}

  {#
      -- in pg_depend, objid is the dependent, refobjid is the referenced object
      --  > a pg_depend entry indicates that the referenced object cannot be
      --  > dropped without also dropping the dependent object.
  #}

  {%- call statement('relations', fetch_result=True) -%}
    select distinct
        dependent_namespace.nspname as dependent_schema,
        dependent_class.relname as dependent_name,
        referenced_namespace.nspname as referenced_schema,
        referenced_class.relname as referenced_name

    -- Query for views: views are entries in pg_class with an entry in pg_rewrite, but we avoid
    -- a seq scan on pg_rewrite by leveraging the fact there is an "internal" row in pg_depend for
    -- the view...
    from pg_class as dependent_class
    join pg_namespace as dependent_namespace on dependent_namespace.oid = dependent_class.relnamespace
    join pg_depend as dependent_depend on dependent_depend.refobjid = dependent_class.oid
        and dependent_depend.classid = 'pg_rewrite'::regclass
        and dependent_depend.refclassid = 'pg_class'::regclass
        and dependent_depend.deptype = 'i'

    -- ... and via pg_depend (that has a row per column, hence the need for "distinct" above, and
    -- making sure to exclude the internal row to avoid a view appearing to depend on itself)...
    join pg_depend as joining_depend on joining_depend.objid = dependent_depend.objid
        and joining_depend.classid = 'pg_rewrite'::regclass
        and joining_depend.refclassid = 'pg_class'::regclass
        and joining_depend.refobjid != dependent_depend.refobjid

    -- ... we can find the tables they query from in pg_class, but excluding system tables. Note we
    -- don't need need to exclude _dependent_ system tables, because they only query from other
    -- system tables, and so are automatically excluded by excluding _referenced_ system tables
    join pg_class as referenced_class on referenced_class.oid = joining_depend.refobjid
    join pg_namespace as referenced_namespace on referenced_namespace.oid = referenced_class.relnamespace
        and referenced_namespace.nspname != 'information_schema'
        and referenced_namespace.nspname not like 'pg\_%'

    order by
        dependent_schema, dependent_name, referenced_schema, referenced_name;

  {%- endcall -%}

  {{ return(load_result('relations').table) }}
{% endmacro %}

{% macro postgres_get_relations() %}
  {{ return(postgres__get_relations()) }}
{% endmacro %}
