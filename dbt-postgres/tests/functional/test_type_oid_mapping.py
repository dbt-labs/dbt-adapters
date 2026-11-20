"""
TYPE_OID_TO_DATA_TYPE names the PostgreSQL built-in types psycopg2 does not register. The names
are what `format_type(oid, null)` returns, so rather than pin them by hand this asks the
catalog of the server under test for every mapped OID and requires agreement.
"""

from dbt.adapters.postgres.connections import TYPE_OID_TO_DATA_TYPE
import psycopg2.extensions

# Every mapped OID exists on PostgreSQL 13 and later; the newest are xid8, pg_snapshot and
# regcollation, all introduced in 13. On an older server they are absent, not wrong.
ALL_MAPPED_OIDS_EXIST_FROM = 130000


class TestTypeOidMapping:
    def test_every_mapped_oid_matches_the_catalog(self, project):
        oids = ", ".join(str(oid) for oid in TYPE_OID_TO_DATA_TYPE)
        rows = project.run_sql(
            f"select oid, format_type(oid, null) from pg_catalog.pg_type where oid in ({oids})",
            fetch="all",
        )
        catalog = {int(oid): name for oid, name in rows}

        divergent = {
            oid: (TYPE_OID_TO_DATA_TYPE[oid], catalog[oid])
            for oid in catalog
            if catalog[oid] != TYPE_OID_TO_DATA_TYPE[oid]
        }
        assert not divergent, "table and catalog disagree:\n" + "\n".join(
            f"  {oid}: table={ours!r} catalog={theirs!r}"
            for oid, (ours, theirs) in sorted(divergent.items())
        )

        (server_version_num,) = project.run_sql("show server_version_num", fetch="one")
        if int(server_version_num) >= ALL_MAPPED_OIDS_EXIST_FROM:
            missing = set(TYPE_OID_TO_DATA_TYPE) - set(catalog)
            assert not missing, f"OIDs {sorted(missing)} are mapped but not in this catalog"

    def test_the_table_only_covers_what_the_driver_does_not(self):
        assert not set(TYPE_OID_TO_DATA_TYPE) & set(psycopg2.extensions.string_types)
