from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
)


class TestPostgresUDFs(UDFsBasic):
    pass


class TestPostgresDeterministicUDFs(DeterministicUDF):
    pass


class TestPostgresStableUDFs(StableUDF):
    pass


class TestPostgresNonDeterministicUDFs(NonDeterministicUDF):
    pass


class TestPostgresErrorForUnsupportedType(ErrorForUnsupportedType):
    pass


class TestPostgresPythonUDFNotSupported(PythonUDFNotSupported):
    pass
