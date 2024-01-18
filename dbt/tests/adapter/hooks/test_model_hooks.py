from pathlib import Path

from dbt.exceptions import ParsingError
from dbt_common.exceptions import CompilationError
import pytest

from dbt.tests.util import run_dbt, write_file
import hooks
import models
import schemas
import seeds
import snapshots


class PrePost:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed_model.sql"))

    def get_ctx_vars(self, state, count, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
            "thread_id",
        ]
        field_list = ", ".join(['"{}"'.format(f) for f in fields])
        query = f"select {field_list} from {project.test_schema}.on_model_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["test_state"] == state
            assert ctx["target_dbname"] == "dbt"
            assert ctx["target_host"] == host
            assert ctx["target_name"] == "default"
            assert ctx["target_schema"] == project.test_schema
            assert ctx["target_threads"] == 4
            assert ctx["target_type"] == "postgres"
            assert ctx["target_user"] == "root"
            assert ctx["target_pass"] == ""

            assert (
                ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
            ), "run_started_at was not set"
            assert (
                ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
            ), "invocation_id was not set"
            assert ctx["thread_id"].startswith("Thread-")


class PrePostModelHooks(PrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        hooks.MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        hooks.MODEL_POST_HOOK,
                    ],
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models.hooks}

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        run_dbt()
        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class PrePostModelHooksUnderscores(PrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre_hook": [
                        # inside transaction (runs second)
                        hooks.MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    "post_hook": [
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        hooks.MODEL_POST_HOOK,
                    ],
                }
            }
        }


class HookRefs(PrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "hooked": {
                        "post-hook": [
                            """
                        insert into {{this.schema}}.on_model_hook select
                        test_state,
                        '{{ target.dbname }}' as target_dbname,
                        '{{ target.host }}' as target_host,
                        '{{ target.name }}' as target_name,
                        '{{ target.schema }}' as target_schema,
                        '{{ target.type }}' as target_type,
                        '{{ target.user }}' as target_user,
                        '{{ target.get(pass, "") }}' as target_pass,
                        {{ target.threads }} as target_threads,
                        '{{ run_started_at }}' as run_started_at,
                        '{{ invocation_id }}' as invocation_id,
                        '{{ thread_id }}' as thread_id
                        from {{ ref('post') }}""".strip()
                        ],
                    }
                },
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": models.hooked,
            "post.sql": models.post,
            "pre.sql": models.pre,
        }

    def test_pre_post_model_hooks_refed(self, project, dbt_profile_target):
        run_dbt()
        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class PrePostModelHooksOnSeeds:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds.example_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": schemas.seed_models}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                    # call any macro to track dependency: https://github.com/dbt-labs/dbt-core/issues/6806
                    "select null::{{ dbt.type_int() }} as id",
                ],
                "quote_columns": False,
            },
        }

    def test_hooks_on_seeds(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["test"])
        assert len(res) == 1, "Expected exactly one item"


class HooksRefsOnSeeds:
    """
    This should not succeed, and raise an explicit error
    https://github.com/dbt-labs/dbt-core/issues/6806
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds.example_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": schemas.seed_models, "post.sql": models.post}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "post-hook": [
                    "select * from {{ ref('post') }}",
                ],
            },
        }

    def test_hook_with_ref_on_seeds(self, project):
        with pytest.raises(ParsingError) as excinfo:
            run_dbt(["parse"])
        assert "Seeds cannot depend on other nodes" in str(excinfo.value)


class PrePostModelHooksOnSeedsPlusPrefixed(PrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class PrePostModelHooksOnSeedsPlusPrefixedWhitespace(PrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class PrePostModelHooksOnSnapshots:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        path = Path(project.project_root) / "test-snapshots"
        Path.mkdir(path)
        write_file(snapshots.test_snapshot, path, "snapshot.sql")

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": schemas.test_snapshot_models}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds.example_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_hooks_on_snapshots(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["snapshot"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["test"])
        assert len(res) == 1, "Expected exactly one item"


class PrePostModelHooksInConfigSetup(PrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models.hooks_configured}


class PrePostModelHooksInConfig(PrePostModelHooksInConfigSetup):
    def test_pre_and_post_model_hooks_model(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class PrePostModelHooksInConfigWithCount(PrePostModelHooksInConfigSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        hooks.MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        hooks.MODEL_POST_HOOK,
                    ],
                }
            }
        }

    def test_pre_and_post_model_hooks_model_and_project(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target.get("host", None), count=2)
        self.check_hooks("end", project, dbt_profile_target.get("host", None), count=2)


class PrePostModelHooksInConfigKwargs(PrePostModelHooksInConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models.hooks_kwargs}


class PrePostSnapshotHooksInConfigKwargs(PrePostModelHooksOnSnapshots):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        path = Path(project.project_root) / "test-kwargs-snapshots"
        Path.mkdir(path)
        write_file(fixtures.snapshots__test_snapshot, path, "snapshot.sql")

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-kwargs-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {
                "quote_columns": False,
            },
        }


class DuplicateHooksInConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models.hooks_error}

    def test_run_duplicate_hook_defs(self, project):
        with pytest.raises(CompilationError) as exc:
            run_dbt()
        assert "pre_hook" in str(exc.value)
        assert "pre-hook" in str(exc.value)
