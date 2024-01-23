from dbt.deprecations import active_deprecations, reset_deprecations
from dbt.tests.util import run_dbt, write_file
from dbt_common.exceptions import CompilationError
import pytest
import yaml

import fixtures


class TestConfigPathDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": fixtures.models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "data-paths": ["data"],
            "log-path": "customlogs",
            "target-path": "customtarget",
        }

    def test_data_path(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        run_dbt(["debug"])
        expected = {
            "project-config-data-paths",
            "project-config-log-path",
            "project-config-target-path",
        }
        assert expected == active_deprecations

    def test_data_path_fail(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        with pytest.raises(CompilationError) as exc:
            run_dbt(["--warn-error", "debug"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `data-paths` config has been renamed"
        assert expected_msg in exc_str


class TestPackageInstallPathDeprecation:
    @pytest.fixture(scope="class")
    def models_trivial(self):
        return {"model.sql": fixtures.models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "clean-targets": ["dbt_modules"]}

    def test_package_path(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        run_dbt(["clean"])
        expected = {"install-packages-path"}
        assert expected == active_deprecations

    def test_package_path_not_set(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        with pytest.raises(CompilationError) as exc:
            run_dbt(["--warn-error", "clean"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "path has changed from `dbt_modules` to `dbt_packages`."
        assert expected_msg in exc_str


class TestPackageRedirectDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": fixtures.models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "fishtown-analytics/dbt_utils", "version": "0.7.0"}]}

    def test_package_redirect(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        run_dbt(["deps"])
        expected = {"package-redirect"}
        assert expected == active_deprecations

    # if this test comes before test_package_redirect it will raise an exception as expected
    def test_package_redirect_fail(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        with pytest.raises(CompilationError) as exc:
            run_dbt(["--warn-error", "deps"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `fishtown-analytics/dbt_utils` package is deprecated in favor of `dbt-labs/dbt_utils`"
        assert expected_msg in exc_str


class TestExposureNameDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": fixtures.models_trivial__model_sql,
            "bad_name.yml": fixtures.bad_name_yaml,
        }

    def test_exposure_name(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        run_dbt(["parse"])
        expected = {"exposure-name"}
        assert expected == active_deprecations

    def test_exposure_name_fail(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        with pytest.raises(CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Starting in v1.3, the 'name' of an exposure should contain only letters, numbers, and underscores."
        assert expected_msg in exc_str


class TestPrjectFlagsMovedDeprecation:
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "config": {"send_anonymous_usage_stats": False},
        }

    @pytest.fixture(scope="class")
    def dbt_project_yml(self, project_root, project_config_update):
        project_config = {
            "name": "test",
            "profile": "test",
        }
        write_file(yaml.safe_dump(project_config), project_root, "dbt_project.yml")
        return project_config

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_profile_config_deprecation(self, project):
        reset_deprecations()
        assert active_deprecations == set()
        run_dbt(["parse"])
        expected = {"project-flags-moved"}
        assert expected == active_deprecations
