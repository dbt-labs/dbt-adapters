from dbt.tests.adapter.simple_copy.test_copy_uppercase import SimpleCopyUppercase
from dbt.tests.adapter.simple_copy.test_simple_copy import (
    EmptyModelsArentRun,
    SimpleCopy,
)


class TestSimpleCopyUppercase(SimpleCopyUppercase):
    pass


class TestSimpleCopyBase(SimpleCopy):
    pass


class TestEmptyModelsArentRun(EmptyModelsArentRun):
    pass
