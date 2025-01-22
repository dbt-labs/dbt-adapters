from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowSqlHeader,
    BaseShowLimit,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestRedshiftShowLimit(BaseShowLimit):
    pass


class TestRedshiftShowSqlHeader(BaseShowSqlHeader):
    pass


class TestShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    pass
