from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
)


class TestPostgresUDFs(UDFsBasic):
    pass


class TestPostgresDeterministicUDFs(DeterministicUDF):
    pass


class TestPostgresStableUDFs(StableUDF):
    pass


class TestPostgresNonDeterministicUDFs(NonDeterministicUDF):
    pass
