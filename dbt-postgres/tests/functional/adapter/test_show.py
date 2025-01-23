from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowLimit,
    BaseShowSqlHeader,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestPostgresShowSqlHeader(BaseShowSqlHeader):
    pass


class TestPostgresShowLimit(BaseShowLimit):
    pass


class TestPostgresShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    pass
