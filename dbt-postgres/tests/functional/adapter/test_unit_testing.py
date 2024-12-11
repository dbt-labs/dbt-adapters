from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestPostgresUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestPostgresUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass


class TestPostgresUnitTestingTypes(BaseUnitTestingTypes):
    pass
