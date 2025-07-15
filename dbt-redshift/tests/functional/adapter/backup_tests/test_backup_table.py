import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.backup_tests import models


class BackupTableBase:
    @pytest.fixture(scope="class", autouse=True)
    def _run_dbt(self, project):
        run_dbt(["run"])


class TestBackupTableOption(BackupTableBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "backup_is_false.sql": models.BACKUP_IS_FALSE,
            "backup_is_true.sql": models.BACKUP_IS_TRUE,
            "backup_is_undefined.sql": models.BACKUP_IS_UNDEFINED,
            "backup_is_true_view.sql": models.BACKUP_IS_TRUE_VIEW,
        }

    @pytest.mark.parametrize(
        "model_ddl,backup_expected",
        [
            ("backup_is_false", False),
            ("backup_is_true", True),
            ("backup_is_undefined", True),
            ("backup_is_true_view", True),
        ],
        indirect=["model_ddl"],
    )
    def test_setting_reflects_config_option(self, model_ddl: str, backup_expected: bool):
        """
        Test different scenarios of configuration at the MODEL level and verify the expected setting for backup

        This test looks for whether `backup no` appears in the DDL file. If it does, then the table will not be backed
        up. If it does not appear, the table will be backed up.

        Args:
            model_ddl: the DDL for each model as a string
            backup_expected: whether backup is expected for this model
        """
        backup_will_occur = "backup no" not in model_ddl.lower()
        assert backup_will_occur == backup_expected


class TestBackupTableSyntax(BackupTableBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "syntax_with_distkey.sql": models.SYNTAX_WITH_DISTKEY,
            "syntax_with_sortkey.sql": models.SYNTAX_WITH_SORTKEY,
        }

    @pytest.mark.parametrize(
        "model_ddl,search_phrase",
        [
            ("syntax_with_distkey", "diststyle key distkey"),
            ("syntax_with_sortkey", "compound sortkey"),
        ],
        indirect=["model_ddl"],
    )
    def test_backup_predicate_precedes_secondary_predicates(self, model_ddl, search_phrase):
        """
        Test whether `backup no` appears roughly in the correct spot in the DDL

        This test verifies that the backup predicate comes before the secondary predicates.
        This test does not guarantee that the resulting DDL is properly formed.

        Args:
            model_ddl: the DDL for each model as a string
            search_phrase: the string within the DDL that indicates the distkey or sortkey
        """
        assert model_ddl.find("backup no") < model_ddl.find(search_phrase)


class TestBackupTableProjectDefault(BackupTableBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"backup": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "backup_is_true.sql": models.BACKUP_IS_TRUE,
            "backup_is_undefined.sql": models.BACKUP_IS_UNDEFINED,
        }

    @pytest.mark.parametrize(
        "model_ddl,backup_expected",
        [("backup_is_true", True), ("backup_is_undefined", False)],
        indirect=["model_ddl"],
    )
    def test_setting_defaults_to_project_option(self, model_ddl: str, backup_expected: bool):
        """
        Test different scenarios of configuration at the PROJECT level and verify the expected setting for backup

        This test looks for whether `backup no` appears in the DDL file. If it does, then the table will not be backed
        up. If it does not appear, the table will be backed up.

        Args:
            model_ddl: the DDL for each model as a string
            backup_expected: whether backup is expected for this model
        """
        backup_will_occur = "backup no" not in model_ddl.lower()
        assert backup_will_occur == backup_expected
