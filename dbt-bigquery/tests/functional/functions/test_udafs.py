from dbt.tests.adapter.functions.test_udafs import (
    BasicSQLUDAF,
    BasicJavaScriptUDAF,
    JavaScriptUDAFDefaultArgSupport,
)


class TestBigQuerySQLUDAF(BasicSQLUDAF):
    pass


class TestBigQueryJavaScriptUDAF(BasicJavaScriptUDAF):
    pass


class TestBigQueryJavaScriptUDAFDefaultArgSupport(JavaScriptUDAFDefaultArgSupport):
    expect_default_arg_support = False
