import uuid

import pytest

from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtDatabaseError


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

    def test_resubmits_terminally_failed_job_under_fresh_id(self, project):
        """A 409 on a job that failed must resubmit under a fresh job_id;
        attaching would only replay the stored error."""
        run_dbt(["run"])

        conns = project.adapter.connections
        table = f"`{project.database}`.`{project.test_schema}`.`failed_probe`"
        query = f"select 1 / (select count(*) from {table}) as n"

        with project.adapter.connection_named("__test_409_failed"):
            conns.raw_execute(f"create or replace table {table} (id int64)")

            fixed_id = f"dbt-conflict-failed-{uuid.uuid4()}"
            original = conns.generate_job_id
            try:
                conns.generate_job_id = lambda: fixed_id
                with pytest.raises(DbtDatabaseError):
                    conns.raw_execute(query)  # 1st: division by zero, job fails

                conns.generate_job_id = original
                conns.raw_execute(f"insert into {table} (id) values (1)")

                # 2nd: same job_id -> 409 on the failed job -> resubmit under a fresh id
                ids = iter([fixed_id])
                conns.generate_job_id = lambda: next(ids, None) or original()
                query_job, iterator = conns.raw_execute(query)
            finally:
                conns.generate_job_id = original

        assert query_job.job_id != fixed_id
        assert list(iterator)[0][0] == 1

    def test_copy_job_resubmits_after_terminal_failure(self, project):
        """Copy jobs have no statement_type; a failed one is still resubmitted."""
        run_dbt(["run"])

        conns = project.adapter.connections
        src = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="failed_copy_src"
        )
        dst = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="failed_copy_dst"
        )
        src_table = f"`{src.database}`.`{src.schema}`.`{src.identifier}`"
        dst_table = f"`{dst.database}`.`{dst.schema}`.`{dst.identifier}`"

        with project.adapter.connection_named("__test_409_copy_failed"):
            conns.raw_execute(f"create or replace table {src_table} as select 1 as id")
            conns.raw_execute(f"create or replace table {dst_table} as select 2 as id")

            fixed_id = f"dbt-conflict-copy-failed-{uuid.uuid4()}"
            original = conns.generate_job_id
            try:
                conns.generate_job_id = lambda: fixed_id
                with pytest.raises(DbtDatabaseError):
                    conns.copy_bq_table(src, dst, "WRITE_EMPTY")  # 1st: destination not empty

                conns.generate_job_id = original
                conns.raw_execute(f"drop table {dst_table}")

                # 2nd: same job_id -> 409 on the failed job -> resubmit under a fresh id
                ids = iter([fixed_id])
                conns.generate_job_id = lambda: next(ids, None) or original()
                conns.copy_bq_table(src, dst, "WRITE_EMPTY")
            finally:
                conns.generate_job_id = original

            _, iterator = conns.raw_execute(f"select id from {dst_table}")
            rows = [row[0] for row in iterator]

        assert rows == [1], f"expected the copied row, got {rows}"
