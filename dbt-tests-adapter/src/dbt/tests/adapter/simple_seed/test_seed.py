from codecs import BOM_UTF8
import csv
from pathlib import Path

import pytest

from dbt.tests.adapter.simple_seed import fixtures, seeds
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_exist,
    check_table_does_not_exist,
    copy_file,
    mkdir,
    read_file,
    rm_dir,
    run_dbt,
)


class SeedConfigBase(object):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }


class SeedTestBase(SeedConfigBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds.seeds__expected_sql)

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {"seed_actual.csv": seeds.seed__actual_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__downstream_from_seed_actual.sql": fixtures.models__downstream_from_seed_actual,
        }

    def _build_relations_for_test(self, project):
        """The testing environment needs seeds and models to interact with"""
        seed_result = run_dbt(["seed"])
        assert len(seed_result) == 1
        check_relations_equal(project.adapter, ["seed_expected", "seed_actual"])

        run_result = run_dbt()
        assert len(run_result) == 1
        check_relations_equal(
            project.adapter, ["models__downstream_from_seed_actual", "seed_expected"]
        )

    def _check_relation_end_state(self, run_result, project, exists: bool):
        assert len(run_result) == 1
        check_relations_equal(project.adapter, ["seed_actual", "seed_expected"])
        if exists:
            check_table_does_exist(project.adapter, "models__downstream_from_seed_actual")
        else:
            check_table_does_not_exist(project.adapter, "models__downstream_from_seed_actual")


class BaseBasicSeedTests(SeedTestBase):
    def test_simple_seed(self, project):
        """Build models and observe that run truncates a seed and re-inserts rows"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)

    def test_simple_seed_full_refresh_flag(self, project):
        """Drop the seed_actual table and re-create. Verifies correct behavior by the absence of the
        model which depends on seed_actual."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=False
        )


class TestBasicSeedTests(BaseBasicSeedTests):
    pass


class BaseSeedConfigFullRefreshOn(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "full_refresh": True},
        }

    def test_simple_seed_full_refresh_config(self, project):
        """config option should drop current model and cascade drop to downstream models"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=False)


class TestSeedConfigFullRefreshOn(BaseSeedConfigFullRefreshOn):
    pass


class BaseSeedConfigFullRefreshOff(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "full_refresh": False},
        }

    def test_simple_seed_full_refresh_config(self, project):
        """Config options should override a full-refresh flag because config is higher priority"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )


class TestSeedConfigFullRefreshOff(BaseSeedConfigFullRefreshOff):
    pass


class BaseSeedCustomSchema(SeedTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds.seeds__expected_sql)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "schema": "custom_schema",
                "quote_columns": False,
            },
        }

    def test_simple_seed_with_schema(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1
        custom_schema = f"{project.test_schema}_custom_schema"
        check_relations_equal(project.adapter, [f"{custom_schema}.seed_actual", "seed_expected"])

        # this should truncate the seed_actual table, then re-insert
        results = run_dbt(["seed"])
        assert len(results) == 1
        custom_schema = f"{project.test_schema}_custom_schema"
        check_relations_equal(project.adapter, [f"{custom_schema}.seed_actual", "seed_expected"])

    def test_simple_seed_with_drop_and_schema(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1
        custom_schema = f"{project.test_schema}_custom_schema"
        check_relations_equal(project.adapter, [f"{custom_schema}.seed_actual", "seed_expected"])

        # this should drop the seed table, then re-create
        results = run_dbt(["seed", "--full-refresh"])
        assert len(results) == 1
        custom_schema = f"{project.test_schema}_custom_schema"
        check_relations_equal(project.adapter, [f"{custom_schema}.seed_actual", "seed_expected"])


class TestSeedCustomSchema(BaseSeedCustomSchema):
    pass


class SeedUniqueDelimiterTestBase(SeedConfigBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": "|"},
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds.seeds__expected_sql)

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {"seed_pipe_separated.csv": seeds.seeds__pipe_separated_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__downstream_from_seed_pipe_separated.sql": fixtures.models__downstream_from_seed_pipe_separated,
        }

    def _build_relations_for_test(self, project):
        """The testing environment needs seeds and models to interact with"""
        seed_result = run_dbt(["seed"])
        assert len(seed_result) == 1
        check_relations_equal(project.adapter, ["seed_expected", "seed_pipe_separated"])

        run_result = run_dbt()
        assert len(run_result) == 1
        check_relations_equal(
            project.adapter, ["models__downstream_from_seed_pipe_separated", "seed_expected"]
        )

    def _check_relation_end_state(self, run_result, project, exists: bool):
        assert len(run_result) == 1
        check_relations_equal(project.adapter, ["seed_pipe_separated", "seed_expected"])
        if exists:
            check_table_does_exist(project.adapter, "models__downstream_from_seed_pipe_separated")
        else:
            check_table_does_not_exist(
                project.adapter, "models__downstream_from_seed_pipe_separated"
            )


class BaseSeedWithUniqueDelimiter(SeedUniqueDelimiterTestBase):
    def test_seed_with_unique_delimiter(self, project):
        """Testing correct run of seeds with a unique delimiter (pipe in this case)"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class TestSeedWithUniqueDelimiter(BaseSeedWithUniqueDelimiter):
    pass


class BaseSeedWithWrongDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ";"},
        }

    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "syntax error" in seed_result.results[0].message.lower()


class TestSeedWithWrongDelimiter(BaseSeedWithWrongDelimiter):
    pass


class BaseSeedWithEmptyDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ""},
        }

    def test_seed_with_empty_delimiter(self, project):
        """Testing failure of running dbt seed with an empty configured delimiter value"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "compilation error" in seed_result.results[0].message.lower()


class TestSeedWithEmptyDelimiter(BaseSeedWithEmptyDelimiter):
    pass


class BaseSimpleSeedEnabledViaConfig:
    @pytest.fixture(scope="session")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds.seeds__enabled_in_config_csv,
            "seed_disabled.csv": seeds.seeds__disabled_in_config_csv,
            "seed_tricky.csv": seeds.seeds__tricky_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {"seed_enabled": {"enabled": True}, "seed_disabled": {"enabled": False}},
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_simple_seed_with_disabled(self, clear_test_schema, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")

    def test_simple_seed_selection(self, clear_test_schema, project):
        results = run_dbt(["seed", "--select", "seed_enabled"])
        assert len(results) == 1
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_not_exist(project.adapter, "seed_tricky")

    def test_simple_seed_exclude(self, clear_test_schema, project):
        results = run_dbt(["seed", "--exclude", "seed_enabled"])
        assert len(results) == 1
        check_table_does_not_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    pass


class BaseSeedParsing(SeedConfigBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds.seeds__expected_sql)

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds.seeds__wont_parse_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": fixtures.models__from_basic_seed}

    def test_dbt_run_skips_seeds(self, project):
        # run does not try to parse the seed files
        assert len(run_dbt()) == 1

        # make sure 'dbt seed' fails, otherwise our test is invalid!
        run_dbt(["seed"], expect_pass=False)


class TestSeedParsing(BaseSeedParsing):
    pass


class BaseSimpleSeedWithBOM(SeedConfigBase):
    # Reference: BOM = byte order mark; see https://www.ibm.com/docs/en/netezza?topic=formats-byte-order-mark
    # Tests for hidden unicode character in csv
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds.seeds__expected_sql)
        copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )

    def test_simple_seed(self, project):
        seed_result = run_dbt(["seed"])
        assert len(seed_result) == 1
        # encoding param must be specified in open, so long as Python reads files with a
        # default file encoding for character sets beyond extended ASCII.
        with open(
            project.project_root / Path("seeds") / Path("seed_bom.csv"), encoding="utf-8"
        ) as fp:
            assert fp.read(1) == BOM_UTF8.decode("utf-8")
        check_relations_equal(project.adapter, ["seed_expected", "seed_bom"])


class TestSimpleSeedWithBOM(BaseSimpleSeedWithBOM):
    pass


class BaseSeedSpecificFormats(SeedConfigBase):
    """Expect all edge cases to build"""

    @staticmethod
    def _make_big_seed(test_data_dir):
        mkdir(test_data_dir)
        big_seed_path = test_data_dir / Path("tmp.csv")
        with open(big_seed_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["seed_id"])
            for i in range(0, 20000):
                writer.writerow([i])
        return big_seed_path

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        big_seed_path = self._make_big_seed(test_data_dir)
        big_seed = read_file(big_seed_path)

        yield {
            "big_seed.csv": big_seed,
            "seed.with.dots.csv": seeds.seed__with_dots_csv,
            "seed_unicode.csv": seeds.seed__unicode_csv,
        }
        rm_dir(test_data_dir)

    def test_simple_seed(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 3


class TestSeedSpecificFormats(BaseSeedSpecificFormats):
    pass


class BaseTestEmptySeed:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"empty_with_header.csv": "a,b,c"}

    def test_empty_seeds(self, project):
        # Should create an empty table and not fail
        results = run_dbt(["seed"])
        assert len(results) == 1


class TestEmptySeed(BaseTestEmptySeed):
    pass
