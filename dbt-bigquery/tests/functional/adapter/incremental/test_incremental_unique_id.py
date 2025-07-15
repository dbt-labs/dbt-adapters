import pytest

from dbt.contracts.results import RunStatus

from dbt.tests.adapter.incremental.test_incremental_unique_id import SubBaseIncrementalUniqueKey


class IncrementalUniqueKeyFalseyNullsEquals(SubBaseIncrementalUniqueKey):
    def test__bad_unique_key(self, project):
        """expect compilation error from unique key not being a column"""

        (status, exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key"
        )

        assert status == RunStatus.Error
        assert "thisisnotacolumn" in exc.lower()

    # test unique_key as list
    def test__empty_unique_key_list(self, project):
        """with no unique keys, seed and model should match"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=9)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="empty_unique_key_list",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__one_unique_key(self, project):
        """with one unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="one_str__overwrite", seed_rows=8, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="str_unique_key",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("one_str__overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__bad_unique_key_list(self, project):
        """expect compilation error from unique key not being a column"""

        (status, exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key_list"
        )

        assert status == RunStatus.Error
        assert "thisisnotacolumn" in exc.lower()


class IncrementalUniqueKeyTruthyNullsEquals(SubBaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_truthy_nulls_equals_macro": True}}

    # no unique_key test
    def test__no_unique_keys(self, project):
        """with no unique keys, seed and model should match"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=9)
        test_case_fields = self.get_test_fields(
            project, seed="seed", incremental_model="no_unique_key", update_sql_file="add_new_rows"
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    # unique_key as str tests
    def test__empty_str_unique_key(self, project):
        """with empty string for unique key, seed and model should match"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=9)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="empty_str_unique_key",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__unary_unique_key_list(self, project):
        """with one unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=8, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="unary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__duplicated_unary_unique_key_list(self, project):
        """with two of the same unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=8, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="duplicated_unary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__trinary_unique_key_list(self, project):
        """with three unique keys, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=8, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="trinary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__trinary_unique_key_list_no_update(self, project):
        """even with three unique keys, adding distinct rows to seed does not
        cause seed and model to diverge"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=9)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="nontyped_trinary_unique_key_list",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


class TestIncrementalUniqueKeyFalseyNullsEquals(IncrementalUniqueKeyFalseyNullsEquals):
    pass


class TestIncrementalUniqueKeyTruthyNullsEquals(IncrementalUniqueKeyTruthyNullsEquals):
    pass
