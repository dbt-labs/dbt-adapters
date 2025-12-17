from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
    SqlUDFDefaultArgSupport,
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


class TestPostgresDefaultArgsSupportSQLUDFs(SqlUDFDefaultArgSupport):
    expect_default_arg_support = True
