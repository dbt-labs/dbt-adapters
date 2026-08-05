"""
Requires a driver-described temp relation to match however the target relation is described.

get_columns_in_temp_relation maps type OIDs through TYPE_OID_TO_DATA_TYPE, and those columns
are diffed against a target described by SHOW COLUMNS (`datasharing` on) or information_schema
(off). Rather than pin captured names, these tests describe the same columns both ways and
require the reported data_type to agree -- get_columns_in_relation already dispatches on the
flag, so each subclass below exercises the pairing that actually occurs.

The two catalogs do not agree on every name (information_schema calls
`interval year to month` `intervaly2m`), which is why the mapping is selected on the flag.

Unit coverage for the map itself is in tests/unit/test_temp_relation_columns.py.
"""

import pytest

from dbt.adapters.redshift import RedshiftRelation
from dbt.adapters.redshift.impl import TYPE_OID_TO_DATA_TYPE

# (column name, DDL type, type OID the driver reports for it)
STANDARD_TYPES = [
    ("c_boolean", "boolean", 16),
    ("c_bigint", "bigint", 20),
    ("c_smallint", "smallint", 21),
    ("c_integer", "integer", 23),
    ("c_real", "real", 700),
    ("c_double", "double precision", 701),
    ("c_char", "char(5)", 1042),
    ("c_varchar", "varchar(20)", 1043),
    ("c_date", "date", 1082),
    ("c_time", "time", 1083),
    ("c_timestamp", "timestamp", 1114),
    ("c_timestamptz", "timestamptz", 1184),
    ("c_timetz", "timetz", 1266),
    ("c_numeric", "numeric(18,2)", 1700),
]

# Probed in a separate table so an unsupported type on an older cluster cannot mask a
# divergence among the standard types. A geometry column reports OID 3999 (GEOMETRYHEX) on the
# wire, never 3000 -- 3999 is a wire representation rather than a declarable type.
REDSHIFT_TYPES = [
    ("c_interval_y2m", "interval year to month", 1188),
    ("c_interval_d2s", "interval day to second", 1190),
    ("c_hllsketch", "hllsketch", 2935),
    ("c_geometry", "geometry", 3999),
    ("c_geography", "geography", 3001),
    ("c_super", "super", 4000),
    ("c_varbyte", "varbyte(16)", 6551),
]

# OID 3000 is the geometry type in pg_type but never appears in a cursor description; Redshift
# aliases TEXT to VARCHAR(256), so no stored column carries OID 25 either.
UNPROBEABLE_OIDS = {25, 3000}


def _column_ddl(types):
    return ", ".join(f"{name} {ddl}" for name, ddl, _ in types)


def _data_types(columns):
    # Column.data_type, not dtype: it folds in size and precision, and is what
    # diff_column_data_types actually compares during schema comparison.
    return {column.column: column.data_type for column in columns}


class DescribersAgree:
    """Describe the same columns via the target's path and via the driver, and compare."""

    def _assert_agree(self, project, types, identifier):
        column_ddl = _column_ddl(types)
        target = RedshiftRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier=identifier,
        )
        project.run_sql(f"create table {target} ({column_ddl})")

        temp_identifier = f"{identifier}__dbt_tmp"
        temp = RedshiftRelation.create(identifier=temp_identifier).include(
            database=False, schema=False
        )
        with project.adapter.connection_named("_test"):
            # get_columns_in_relation dispatches on datasharing, so this is whichever
            # describer the incremental materialization would have used for the target.
            from_target = _data_types(project.adapter.get_columns_in_relation(target))
            # Temp relations are session-scoped: create and describe on the same connection.
            project.adapter.execute(f"create temp table {temp_identifier} ({column_ddl})")
            from_driver = _data_types(project.adapter.get_columns_in_temp_relation(temp))

        divergences = {
            name: (from_target.get(name), from_driver.get(name))
            for name, _, _ in types
            if from_target.get(name) != from_driver.get(name)
        }
        assert not divergences, "target and driver disagree:\n" + "\n".join(
            f"  {name}: target={target_type!r} driver={driver_type!r}"
            for name, (target_type, driver_type) in divergences.items()
        )

    def test_standard_types_agree(self, project):
        self._assert_agree(project, STANDARD_TYPES, "type_probe_standard")

    def test_redshift_types_agree(self, project):
        self._assert_agree(project, REDSHIFT_TYPES, "type_probe_redshift")


class TestDescribersAgreeDatasharingOff(DescribersAgree):
    """Target described by information_schema, via the legacy path."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_type_oid_mapping_datasharing_off"}


class TestDescribersAgreeDatasharingOn(DescribersAgree):
    """Target described by SHOW COLUMNS."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_type_oid_mapping_datasharing_on"}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }


def test_every_mapped_oid_is_probed():
    probed = {oid for _, _, oid in STANDARD_TYPES + REDSHIFT_TYPES}
    unprobed = set(TYPE_OID_TO_DATA_TYPE) - probed - UNPROBEABLE_OIDS
    assert not unprobed, (
        f"OIDs {sorted(unprobed)} are mapped but not probed against the catalogs. Add them "
        f"to STANDARD_TYPES/REDSHIFT_TYPES, or to UNPROBEABLE_OIDS with a reason."
    )
