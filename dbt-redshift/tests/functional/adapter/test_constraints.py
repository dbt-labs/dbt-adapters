import pytest
from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
    BaseConstraintQuotedColumn,
)

_expected_sql_redshift = """
create table <model_identifier> (
    id integer not null primary key references <foreign_key_model_identifier> (id) unique,
    color text,
    date_day text
) ;
insert into <model_identifier>
(
    select
        id,
        color,
        date_day from
    (
        -- depends_on: <foreign_key_model_identifier>
        select
            'blue' as color,
            1 as id,
            '2019-01-01' as date_day
    ) as model_subq
)
;
"""


class RedshiftColumnEqualSetup:
    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # NOTE: Unlike some other adapters, we don't test array or JSON types here, because
        # Redshift does not support them as materialized table column types.

        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "TIMESTAMPTZ"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "TIMESTAMP"],
            ["'1'::numeric", "numeric", "NUMERIC"],
        ]


class TestRedshiftTableConstraintsColumnsEqual(
    RedshiftColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestRedshiftViewConstraintsColumnsEqual(
    RedshiftColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestRedshiftIncrementalConstraintsColumnsEqual(
    RedshiftColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass


class TestRedshiftTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_redshift


class TestRedshiftTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Cannot insert a NULL value into column id"]


class TestRedshiftIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_redshift


class TestRedshiftIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Cannot insert a NULL value into column id"]


class TestRedshiftModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer not null,
    color text,
    date_day text,
    primary key (id),
    constraint strange_uniqueness_requirement unique (color, date_day),
    foreign key (id) references <foreign_key_model_identifier> (id)
) ;
insert into <model_identifier>
(
    select
        id,
        color,
        date_day from
    (
        -- depends_on: <foreign_key_model_identifier>
        select
            'blue' as color,
            1 as id,
            '2019-01-01' as date_day
    ) as model_subq
)
;
"""


class TestRedshiftConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer not null,
    "from" text not null,
    date_day text
) ;
insert into <model_identifier>
(
    select id, "from", date_day
    from (
        select
          'blue' as "from",
          1 as id,
          '2019-01-01' as date_day
    ) as model_subq
);
"""
