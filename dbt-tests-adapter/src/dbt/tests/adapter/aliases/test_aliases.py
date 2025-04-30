import pytest

from dbt.tests.adapter.aliases import fixtures
from dbt.tests.util import run_dbt


class BaseAliases:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias_in_project": {
                        "alias": "project_alias",
                    },
                    "alias_in_project_with_override": {
                        "alias": "project_alias",
                    },
                }
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixtures.MODELS__SCHEMA_YML,
            "foo_alias.sql": fixtures.MODELS__FOO_ALIAS_SQL,
            "alias_in_project.sql": fixtures.MODELS__ALIAS_IN_PROJECT_SQL,
            "alias_in_project_with_override.sql": fixtures.MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL,
            "ref_foo_alias.sql": fixtures.MODELS__REF_FOO_ALIAS_SQL,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": fixtures.MACROS__CAST_SQL,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }

    def test_alias_model_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4
        run_dbt(["test"])


class BaseAliasErrors:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": fixtures.MACROS__CAST_SQL,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": fixtures.MODELS_DUPE__MODEL_A_SQL,
            "model_b.sql": fixtures.MODELS_DUPE__MODEL_B_SQL,
        }

    def test_alias_dupe_throws_exeption(self, project):
        message = ".*identical database representation.*"
        with pytest.raises(Exception) as exc:
            assert message in exc
            run_dbt(["run"])


class BaseSameAliasDifferentSchemas:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": fixtures.MACROS__CAST_SQL,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixtures.MODELS_DUPE_CUSTOM_SCHEMA__SCHEMA_YML,
            "model_a.sql": fixtures.MODELS_DUPE_CUSTOM_SCHEMA__MODEL_A_SQL,
            "model_b.sql": fixtures.MODELS_DUPE_CUSTOM_SCHEMA__MODEL_B_SQL,
            "model_c.sql": fixtures.MODELS_DUPE_CUSTOM_SCHEMA__MODEL_C_SQL,
        }

    def test_same_alias_succeeds_in_different_schemas(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        res = run_dbt(["test"])
        assert len(res) > 0


class BaseSameAliasDifferentDatabases:
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias": "duped_alias",
                    "model_b": {"schema": unique_schema + "_alt"},
                },
            },
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": fixtures.MACROS__CAST_SQL,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixtures.MODELS_DUPE_CUSTOM_DATABASE__SCHEMA_YML,
            "model_a.sql": fixtures.MODELS_DUPE_CUSTOM_DATABASE__MODEL_A_SQL,
            "model_b.sql": fixtures.MODELS_DUPE_CUSTOM_DATABASE__MODEL_B_SQL,
        }

    def test_alias_model_name_diff_database(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        res = run_dbt(["test"])
        assert len(res) > 0


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    pass
