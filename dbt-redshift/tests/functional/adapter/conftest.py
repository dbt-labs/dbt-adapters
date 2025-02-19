from typing import Generator

import pytest


@pytest.fixture
def model_ddl(request) -> Generator[str, None, None]:
    """
    Returns the contents of the DDL file for the model provided. Use with pytest parameterization.

    Example:
    ===
    @pytest.mark.parametrize(
        "model_ddl,backup_expected",
        [("backup_is_false", False)],
        indirect=["model_ddl"]
    )
    def test_setting_reflects_config_option(self, model_ddl: str, backup_expected: bool):
        backup_will_occur = "backup no" not in model_ddl.lower()
        assert backup_will_occur == backup_expected
    ===

    In this example, the fixture returns the contents of the backup_is_false DDL file as a string.
    This string is then referenced in the test as model_ddl.
    """
    with open(f"target/run/test/models/{request.param}.sql", "r") as ddl_file:
        yield "\n".join(ddl_file.readlines())
