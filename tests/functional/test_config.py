from argparse import Namespace
from contextlib import contextmanager
import os
import shutil
import tempfile
from unittest import TestCase, mock

import dbt.config
import dbt.exceptions
import dbt.tracking
import pytest
import yaml

from dbt.adapters.postgres import PostgresCredentials
from tests.functional.utils import normalize


INITIAL_ROOT = os.getcwd()


@contextmanager
def temp_cd(path):
    current_path = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current_path)


@contextmanager
def raises_nothing():
    yield


def empty_profile_renderer():
    return dbt.config.renderer.ProfileRenderer({})


def empty_project_renderer():
    return dbt.config.renderer.DbtProjectYamlRenderer()


model_config = {
    "my_package_name": {
        "enabled": True,
        "adwords": {
            "adwords_ads": {"materialized": "table", "enabled": True, "schema": "analytics"}
        },
        "snowplow": {
            "snowplow_sessions": {
                "sort": "timestamp",
                "materialized": "incremental",
                "dist": "user_id",
                "unique_key": "id",
            },
            "base": {
                "snowplow_events": {
                    "sort": ["timestamp", "userid"],
                    "materialized": "table",
                    "sort_type": "interleaved",
                    "dist": "userid",
                }
            },
        },
    }
}

model_fqns = frozenset(
    (
        ("my_package_name", "snowplow", "snowplow_sessions"),
        ("my_package_name", "snowplow", "base", "snowplow_events"),
        ("my_package_name", "adwords", "adwords_ads"),
    )
)


class Args:
    def __init__(
        self,
        profiles_dir=None,
        threads=None,
        profile=None,
        cli_vars=None,
        version_check=None,
        project_dir=None,
        target=None,
    ):
        self.profile = profile
        self.threads = threads
        self.target = target
        if profiles_dir is not None:
            self.profiles_dir = profiles_dir
        if cli_vars is not None:
            self.vars = cli_vars
        if version_check is not None:
            self.version_check = version_check
        if project_dir is not None:
            self.project_dir = project_dir


class BaseConfigTest(TestCase):
    """Subclass this, and before calling the superclass setUp, set
    self.profiles_dir and self.project_dir.
    """

    def setUp(self):
        # Write project
        self.project_dir = normalize(tempfile.mkdtemp())
        self.default_project_data = {
            "version": "0.0.1",
            "name": "my_test_project",
            "profile": "default",
        }
        self.write_project(self.default_project_data)

        # Write profile
        self.profiles_dir = normalize(tempfile.mkdtemp())
        self.default_profile_data = {
            "default": {
                "outputs": {
                    "postgres": {
                        "type": "postgres",
                        "host": "postgres-db-hostname",
                        "port": 5555,
                        "user": "db_user",
                        "pass": "db_pass",
                        "dbname": "postgres-db-name",
                        "schema": "postgres-schema",
                        "threads": 7,
                    },
                    "with-vars": {
                        "type": "{{ env_var('env_value_type') }}",
                        "host": "{{ env_var('env_value_host') }}",
                        "port": "{{ env_var('env_value_port') | as_number }}",
                        "user": "{{ env_var('env_value_user') }}",
                        "pass": "{{ env_var('env_value_pass') }}",
                        "dbname": "{{ env_var('env_value_dbname') }}",
                        "schema": "{{ env_var('env_value_schema') }}",
                    },
                    "cli-and-env-vars": {
                        "type": "{{ env_var('env_value_type') }}",
                        "host": "{{ var('cli_value_host') }}",
                        "port": "{{ env_var('env_value_port') | as_number }}",
                        "user": "{{ env_var('env_value_user') }}",
                        "pass": "{{ env_var('env_value_pass') }}",
                        "dbname": "{{ env_var('env_value_dbname') }}",
                        "schema": "{{ env_var('env_value_schema') }}",
                    },
                },
                "target": "postgres",
            },
            "other": {
                "outputs": {
                    "other-postgres": {
                        "type": "postgres",
                        "host": "other-postgres-db-hostname",
                        "port": 4444,
                        "user": "other_db_user",
                        "pass": "other_db_pass",
                        "dbname": "other-postgres-db-name",
                        "schema": "other-postgres-schema",
                        "threads": 2,
                    }
                },
                "target": "other-postgres",
            },
            "empty_profile_data": {},
        }
        self.write_profile(self.default_profile_data)

        self.args = Namespace(
            profiles_dir=self.profiles_dir,
            cli_vars={},
            version_check=True,
            project_dir=self.project_dir,
            target=None,
            threads=None,
            profile=None,
        )
        self.env_override = {
            "env_value_type": "postgres",
            "env_value_host": "env-postgres-host",
            "env_value_port": "6543",
            "env_value_user": "env-postgres-user",
            "env_value_pass": "env-postgres-pass",
            "env_value_dbname": "env-postgres-dbname",
            "env_value_schema": "env-postgres-schema",
            "env_value_profile": "default",
        }

    def assertRaisesOrReturns(self, exc):
        if exc is None:
            return raises_nothing()
        else:
            return self.assertRaises(exc)

    def tearDown(self):
        try:
            shutil.rmtree(self.project_dir)
        except EnvironmentError:
            pass
        try:
            shutil.rmtree(self.profiles_dir)
        except EnvironmentError:
            pass

    def project_path(self, name):
        return os.path.join(self.project_dir, name)

    def profile_path(self, name):
        return os.path.join(self.profiles_dir, name)

    def write_project(self, project_data=None):
        if project_data is None:
            project_data = self.project_data
        with open(self.project_path("dbt_project.yml"), "w") as fp:
            yaml.dump(project_data, fp)

    def write_packages(self, package_data):
        with open(self.project_path("packages.yml"), "w") as fp:
            yaml.dump(package_data, fp)

    def write_profile(self, profile_data=None):
        if profile_data is None:
            profile_data = self.profile_data
        with open(self.profile_path("profiles.yml"), "w") as fp:
            yaml.dump(profile_data, fp)

    def write_empty_profile(self):
        with open(self.profile_path("profiles.yml"), "w") as fp:
            yaml.dump("", fp)


class TestProfile(BaseConfigTest):
    def from_raw_profiles(self):
        renderer = empty_profile_renderer()
        return dbt.config.Profile.from_raw_profiles(self.default_profile_data, "default", renderer)

    def test_from_raw_profiles(self):
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "postgres")
        self.assertEqual(profile.threads, 7)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "postgres-db-hostname")
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, "db_user")
        self.assertEqual(profile.credentials.password, "db_pass")
        self.assertEqual(profile.credentials.schema, "postgres-schema")
        self.assertEqual(profile.credentials.database, "postgres-db-name")

    def test_missing_type(self):
        del self.default_profile_data["default"]["outputs"]["postgres"]["type"]
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn("type", str(exc.exception))
        self.assertIn("postgres", str(exc.exception))
        self.assertIn("default", str(exc.exception))

    def test_bad_type(self):
        self.default_profile_data["default"]["outputs"]["postgres"]["type"] = "invalid"
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn("Credentials", str(exc.exception))
        self.assertIn("postgres", str(exc.exception))
        self.assertIn("default", str(exc.exception))

    def test_invalid_credentials(self):
        del self.default_profile_data["default"]["outputs"]["postgres"]["host"]
        with self.assertRaises(dbt.exceptions.DbtProfileError) as exc:
            self.from_raw_profiles()
        self.assertIn("Credentials", str(exc.exception))
        self.assertIn("postgres", str(exc.exception))
        self.assertIn("default", str(exc.exception))

    def test_missing_target(self):
        profile = self.default_profile_data["default"]
        del profile["target"]
        profile["outputs"]["default"] = profile["outputs"]["postgres"]
        profile = self.from_raw_profiles()
        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "default")
        self.assertEqual(profile.credentials.type, "postgres")


@pytest.mark.skip("Flags() has no attribute PROFILES_DIR")
class TestProfileFile(BaseConfigTest):
    def from_raw_profile_info(self, raw_profile=None, profile_name="default", **kwargs):
        if raw_profile is None:
            raw_profile = self.default_profile_data["default"]
        renderer = empty_profile_renderer()
        kw = {
            "raw_profile": raw_profile,
            "profile_name": profile_name,
            "renderer": renderer,
        }
        kw.update(kwargs)
        return dbt.config.Profile.from_raw_profile_info(**kw)

    def from_args(self, project_profile_name="default", **kwargs):
        kw = {
            "project_profile_name": project_profile_name,
            "renderer": empty_profile_renderer(),
            "threads_override": self.args.threads,
            "target_override": self.args.target,
            "profile_name_override": self.args.profile,
        }
        kw.update(kwargs)
        return dbt.config.Profile.render(**kw)

    def test_profile_simple(self):
        profile = self.from_args()
        from_raw = self.from_raw_profile_info()

        self.assertEqual(profile.target_name, "postgres")
        self.assertEqual(profile.threads, 3)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "postgres-db-hostname")
        self.assertEqual(profile.credentials.port, 5555)
        self.assertEqual(profile.credentials.user, "db_user")
        self.assertEqual(profile.credentials.password, "db_pass")
        self.assertEqual(profile.credentials.schema, "postgres-schema")
        self.assertEqual(profile.credentials.database, "postgres-db-name")
        self.assertEqual(profile, from_raw)

    def test_profile_override(self):
        self.args.profile = "other"
        self.args.threads = 3
        profile = self.from_args()
        from_raw = self.from_raw_profile_info(
            self.default_profile_data["other"],
            "other",
            threads_override=3,
        )

        self.assertEqual(profile.target_name, "other-postgres")
        self.assertEqual(profile.threads, 3)
        self.assertTrue(isinstance(profile.credentials, PostgresCredentials))
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "other-postgres-db-hostname")
        self.assertEqual(profile.credentials.port, 4444)
        self.assertEqual(profile.credentials.user, "other_db_user")
        self.assertEqual(profile.credentials.password, "other_db_pass")
        self.assertEqual(profile.credentials.schema, "other-postgres-schema")
        self.assertEqual(profile.credentials.database, "other-postgres-db-name")
        self.assertEqual(profile, from_raw)

    def test_env_vars(self):
        self.args.target = "with-vars"
        with mock.patch.dict(os.environ, self.env_override):
            profile = self.from_args()
            from_raw = self.from_raw_profile_info(target_override="with-vars")

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "with-vars")
        self.assertEqual(profile.threads, 1)
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "env-postgres-host")
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, "env-postgres-user")
        self.assertEqual(profile.credentials.password, "env-postgres-pass")
        self.assertEqual(profile, from_raw)

    def test_env_vars_env_target(self):
        self.default_profile_data["default"]["target"] = "{{ env_var('env_value_target') }}"
        self.write_profile(self.default_profile_data)
        self.env_override["env_value_target"] = "with-vars"
        with mock.patch.dict(os.environ, self.env_override):
            profile = self.from_args()
            from_raw = self.from_raw_profile_info(target_override="with-vars")

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "with-vars")
        self.assertEqual(profile.threads, 1)
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "env-postgres-host")
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, "env-postgres-user")
        self.assertEqual(profile.credentials.password, "env-postgres-pass")
        self.assertEqual(profile, from_raw)

    def test_cli_and_env_vars(self):
        self.args.target = "cli-and-env-vars"
        self.args.vars = {"cli_value_host": "cli-postgres-host"}
        renderer = dbt.config.renderer.ProfileRenderer({"cli_value_host": "cli-postgres-host"})
        with mock.patch.dict(os.environ, self.env_override):
            profile = self.from_args(renderer=renderer)
            from_raw = self.from_raw_profile_info(
                target_override="cli-and-env-vars",
                renderer=renderer,
            )

        self.assertEqual(profile.profile_name, "default")
        self.assertEqual(profile.target_name, "cli-and-env-vars")
        self.assertEqual(profile.threads, 1)
        self.assertEqual(profile.credentials.type, "postgres")
        self.assertEqual(profile.credentials.host, "cli-postgres-host")
        self.assertEqual(profile.credentials.port, 6543)
        self.assertEqual(profile.credentials.user, "env-postgres-user")
        self.assertEqual(profile.credentials.password, "env-postgres-pass")
        self.assertEqual(profile, from_raw)
