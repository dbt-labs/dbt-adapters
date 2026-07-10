import uuid

import pytest

from dbt.tests.util import run_dbt


_SEED = "select 1 as id"


class TestStableJobIdAttachesOnConflict:
    """End-to-end proof that a re-used job_id triggers a real BigQuery 409 and
    that dbt attaches to the in-flight/finished job (via get_job) instead of
    resubmitting non-idempotent DML a second time (inc-6741 / PR #2054).

    With the old behavior (fresh job_id per attempt) the INSERT would run twice
    and the row count would be 2. With the stable job_id + 409-attach fix it
    stays 1.
    """

    @pytest.fixture(scope="class")
    def models(self):
        # A trivial model just to get a schema/dataset created for the test.
        return {"anchor.sql": _SEED}

    def test_resubmit_same_job_id_does_not_duplicate(self, project):
        run_dbt(["run"])

        conns = project.adapter.connections
        table = f"`{project.database}`.`{project.test_schema}`.`conflict_probe`"

        with project.adapter.connection_named("__test_409"):
            conns.raw_execute(f"create or replace table {table} (id int64)")

            # Pin the job_id so the second submission collides in BigQuery.
            fixed_id = f"dbt-conflict-test-{uuid.uuid4()}"
            original = conns.generate_job_id
            conns.generate_job_id = lambda: fixed_id
            try:
                dml = f"insert into {table} (id) values (1)"
                conns.raw_execute(dml)  # 1st: real insert, job created
                conns.raw_execute(dml)  # 2nd: same job_id -> 409 -> attach, no re-insert
            finally:
                conns.generate_job_id = original

            _, iterator = conns.raw_execute(f"select count(*) as n from {table}")
            count = list(iterator)[0][0]

        assert count == 1, f"expected 1 row (attached to existing job), got {count}"

    def test_copy_job_resubmit_attaches(self, project):
        """copy_bq_table has no built-in 409 recovery; verify _submit_or_attach
        keeps a resubmitted copy from failing the run."""
        run_dbt(["run"])

        conns = project.adapter.connections
        src = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="copy_src"
        )
        dst = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="copy_dst"
        )

        with project.adapter.connection_named("__test_409_copy"):
            conns.raw_execute(
                f"create or replace table `{src.database}`.`{src.schema}`.`{src.identifier}` "
                "as select 1 as id"
            )

            fixed_id = f"dbt-conflict-copy-{uuid.uuid4()}"
            original = conns.generate_job_id
            conns.generate_job_id = lambda: fixed_id
            try:
                conns.copy_bq_table(src, dst, "WRITE_TRUNCATE")
                # Resubmit with the same job_id -> 409 -> attach, must not raise.
                conns.copy_bq_table(src, dst, "WRITE_TRUNCATE")
            finally:
                conns.generate_job_id = original

            _, iterator = conns.raw_execute(
                f"select count(*) as n from `{dst.database}`.`{dst.schema}`.`{dst.identifier}`"
            )
            count = list(iterator)[0][0]

        assert count == 1, f"expected 1 row in copy destination, got {count}"
