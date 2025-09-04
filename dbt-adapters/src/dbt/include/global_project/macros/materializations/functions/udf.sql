{% macro get_udf_build_sql() %}
    CREATE FUNCTION price_for_xlarge (price integer) RETURNS integer AS $$
        SELECT 2 * price;
    $$ LANGUAGE SQL;
{% endmacro %}
