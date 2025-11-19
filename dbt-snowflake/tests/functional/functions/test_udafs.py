from dbt.tests.adapter.functions.test_udafs import BasicPythonUDAF, PythonUDAFDefaultArgSupport


class TestSnowflakePythonUDAF(BasicPythonUDAF):
    pass


class TestSnowflakePythonUDAFDefaultArgSupport(PythonUDAFDefaultArgSupport):
    expect_default_arg_support = True
