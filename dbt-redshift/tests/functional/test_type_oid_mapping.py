"""
Pins TYPE_OID_TO_DATA_TYPE against the catalogs that actually describe target relations.

``get_columns_in_temp_relation`` describes a temp relation from the driver's cursor
description, mapping type OIDs through ``TYPE_OID_TO_DATA_TYPE``. Those columns are then
diffed against the target relation, which is described by one of two *other* paths depending
on the ``datasharing`` profile flag:

    | datasharing | target described by                      | reports            |
    |-------------|------------------------------------------|--------------------|
    | true        | redshift__get_columns_in_relation_show   | SHOW COLUMNS       |
    | false       | redshift__get_columns_in_relation_legacy | information_schema |

The map has to agree with whichever one ran, or every column of that type reads as changed
on every run. Its values were captured from SHOW COLUMNS, so this test is what establishes
that information_schema agrees rather than assuming it does.

Comparing information_schema directly is faithful to the legacy macro: for a regular table
its ``bound_views`` CTE selects ``data_type`` from information_schema and the final select
passes it through unchanged.

Types are declared with DDL rather than built from casts so the declared type is
unambiguous. Unit coverage for the map itself is in tests/unit/test_temp_relation_columns.py.
"""

from dbt.adapters.redshift import RedshiftRelation
from dbt.adapters.redshift.impl import TYPE_OID_TO_DATA_TYPE

# (column name, DDL type, data type name the catalogs are expected to report, type OID)
#
# SQL standard types. Both catalogs report the standard name for these, and the legacy macro
# already depends on that -- its unbound_views CTE normalizes to the literal strings
# 'character varying' and 'numeric'.
STANDARD_TYPES = [
    ("c_boolean", "boolean", "boolean", 16),
    ("c_bigint", "bigint", "bigint", 20),
    ("c_smallint", "smallint", "smallint", 21),
    ("c_integer", "integer", "integer", 23),
    ("c_real", "real", "real", 700),
    ("c_double", "double precision", "double precision", 701),
    ("c_char", "char(5)", "character", 1042),
    ("c_varchar", "varchar(20)", "character varying", 1043),
    ("c_date", "date", "date", 1082),
    ("c_time", "time", "time without time zone", 1083),
    ("c_timestamp", "timestamp", "timestamp without time zone", 1114),
    ("c_timestamptz", "timestamptz", "timestamp with time zone", 1184),
    ("c_timetz", "timetz", "time with time zone", 1266),
    ("c_numeric", "numeric(18,2)", "numeric", 1700),
]

# Redshift-proprietary types. These are the entries in doubt: information_schema is inherited
# from Postgres 8.0.2, which reports 'USER-DEFINED' for types it does not recognize, so
# agreement with SHOW COLUMNS is exactly what needs demonstrating rather than assuming.
# Kept in their own probe table so an unsupported type on an older cluster cannot mask a
# divergence among the standard types above.
REDSHIFT_TYPES = [
    ("c_interval_y2m", "interval year to month", "interval year to month", 1188),
    ("c_interval_d2s", "interval day to second", "interval day to second", 1190),
    ("c_hllsketch", "hllsketch", "hllsketch", 2935),
    ("c_geometry", "geometry", "geometry", 3000),
    ("c_geography", "geography", "geography", 3001),
    ("c_super", "super", "super", 4000),
    ("c_varbyte", "varbyte(16)", "binary varying", 6551),
]

# OID 25 (text) is deliberately absent from both lists: Redshift treats TEXT as an alias for
# VARCHAR(256), so no stored column ever carries that OID and there is no catalog row to
# compare against. The driver only produces it for expression results, which is why the map's
# entry for it is an inference rather than a capture.
UNPROBEABLE_OIDS = {25}


def _column_ddl(types):
    return ", ".join(f"{name} {ddl}" for name, ddl, _, _ in types)


def _show_columns_data_types(project, relation):
    """data_type per column as SHOW COLUMNS reports it, keyed by column name."""
    with project.adapter.connection_named("_test"):
        _, table = project.adapter.execute(
            f"show columns from table {relation}",
            fetch=True,
        )
    return {row["column_name"]: row["data_type"] for row in table.rows}


def _information_schema_data_types(project, identifier):
    """data_type per column as information_schema reports it, keyed by column name."""
    rows = project.run_sql(
        f"""
        select column_name, data_type
        from information_schema.columns
        where table_schema = '{project.test_schema}'
          and table_name = '{identifier}'
        """,
        fetch="all",
    )
    return {name: data_type for name, data_type in rows}


def _driver_data_types(project, identifier, column_ddl):
    """dtype per column as the driver-based fallback reports it, keyed by column name.

    The temp relation has to be created and described on the same connection, since temp
    relations are session-scoped.
    """
    relation = RedshiftRelation.create(identifier=identifier).include(database=False, schema=False)
    with project.adapter.connection_named("_test"):
        project.adapter.execute(f"create temp table {identifier} ({column_ddl})")
        columns = project.adapter.get_columns_in_temp_relation(relation)
    return {column.column: column.dtype for column in columns}


class TestTypeOidMappingMatchesCatalogs:
    def _assert_describers_agree(self, project, types, identifier):
        column_ddl = _column_ddl(types)
        relation = RedshiftRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier=identifier,
        )
        project.run_sql(f"create table {relation} ({column_ddl})")

        described = {
            "SHOW COLUMNS": _show_columns_data_types(project, relation),
            "information_schema": _information_schema_data_types(project, identifier),
            "driver": _driver_data_types(project, f"{identifier}__dbt_tmp", column_ddl),
        }

        divergences = []
        for name, ddl, expected, oid in types:
            reported = {"TYPE_OID_TO_DATA_TYPE": TYPE_OID_TO_DATA_TYPE.get(oid)}
            reported.update({source: names.get(name) for source, names in described.items()})
            # Any disagreement matters, including all three catalogs agreeing on a name the
            # map does not carry -- that is still a type change on every run.
            if len(set(reported.values())) > 1:
                divergences.append((ddl, oid, expected, reported))

        assert not divergences, "describers disagree:\n" + "\n".join(
            f"  {ddl} (OID {oid}, captured as {expected!r}): "
            + ", ".join(f"{source}={value!r}" for source, value in reported.items())
            for ddl, oid, expected, reported in divergences
        )

    def test_standard_types_agree(self, project):
        self._assert_describers_agree(project, STANDARD_TYPES, "type_probe_standard")

    def test_redshift_types_agree(self, project):
        self._assert_describers_agree(project, REDSHIFT_TYPES, "type_probe_redshift")

    def test_every_mapped_oid_is_probed(self):
        """A new entry in the map must come with evidence, or be explicitly exempted."""
        probed = {oid for _, _, _, oid in STANDARD_TYPES + REDSHIFT_TYPES}
        unprobed = set(TYPE_OID_TO_DATA_TYPE) - probed - UNPROBEABLE_OIDS
        assert not unprobed, (
            f"OIDs {sorted(unprobed)} are mapped but not probed against the catalogs. Add "
            f"them to STANDARD_TYPES/REDSHIFT_TYPES, or to UNPROBEABLE_OIDS with a reason."
        )
