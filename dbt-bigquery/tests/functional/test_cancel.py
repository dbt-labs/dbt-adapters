import platform

import time

import os
import signal
import subprocess
import threading

import pytest

from dbt.tests.util import get_connection

# How long to wait for the model to start before sending SIGINT. If the "START"
# line is never observed (some CI runners do not deliver the child's piped
# output promptly), we fall back to signalling after this delay so a missed
# line can never turn into a full hang.
_START_TIMEOUT = 120.0
# Hard cap on how long we wait for dbt to exit after SIGINT. Cancellation must
# terminate the run promptly; if it does not, fail fast with diagnostics rather
# than hanging until the CI job timeout.
_EXIT_TIMEOUT = 120.0
# How often to resend SIGINT (and re-check for exit) while waiting for dbt to
# cancel and terminate.
_SIGINT_INTERVAL = 5.0

_SEED_CSV = """
id, name, astrological_sign, moral_alignment
1, Alice, Aries, Lawful Good
2, Bob, Taurus, Neutral Good
3, Thaddeus, Gemini, Chaotic Neutral
4, Zebulon, Cancer, Lawful Evil
5, Yorick, Leo, True Neutral
6, Xavier, Virgo, Chaotic Evil
7, Wanda, Libra, Lawful Neutral
"""

_LONG_RUNNING_MODEL_SQL = """
    {{ config(materialized='table') }}
    with array_1 as (
    select generated_ids from UNNEST(GENERATE_ARRAY(1, 200000)) AS generated_ids
    ),
    array_2 as (
    select generated_ids from UNNEST(GENERATE_ARRAY(2, 200000)) AS generated_ids
    )

    SELECT array_1.generated_ids
    FROM array_1
    LEFT JOIN array_1 as jnd on 1=1
    LEFT JOIN array_2 as jnd2 on 1=1
    LEFT JOIN array_1 as jnd3 on jnd3.generated_ids >= jnd2.generated_ids
"""


def _get_info_schema_jobs_query(project_id, dataset_id, table_id):
    """
    Running this query requires roles/bigquery.resourceViewer on the project,
    see: https://cloud.google.com/bigquery/docs/information-schema-jobs#required_role
    :param project_id:
    :param dataset_id:
    :param table_id:
    :return: a single job id that matches the model we tried to create and was cancelled
    """
    return f"""
        SELECT job_id
        FROM `region-us`.`INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 HOUR)
        AND statement_type = 'CREATE_TABLE_AS_SELECT'
        AND state = 'DONE'
        AND job_id IS NOT NULL
        AND project_id = '{project_id}'
        AND error_result.reason = 'stopped'
        AND error_result.message = 'Job execution was cancelled: User requested cancellation'
        AND destination_table.table_id = '{table_id}'
        AND destination_table.dataset_id = '{dataset_id}'
    """


def _unblock_sigint_in_child():
    """Ensure the dbt child starts with SIGINT unblocked.

    This test runs under pytest-xdist (`-n`), whose worker processes run with
    SIGINT blocked in their signal mask. `subprocess` inherits the parent
    thread's signal *mask* (it only resets SIG_IGN dispositions, not the mask),
    so without this the dbt child inherits a blocked SIGINT: the SIGINT we send
    stays pending at the OS level, is never delivered to any thread, and dbt's
    KeyboardInterrupt handling never runs. Unblocking here (in the forked child,
    before exec) restores normal Ctrl-C behavior.
    """
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})


def _run_dbt_in_subprocess(project, dbt_command):

    # Diagnostic: report whether the (xdist worker) parent has SIGINT blocked,
    # which the child would otherwise inherit. Shown in pytest's captured stdout.
    parent_mask = signal.pthread_sigmask(signal.SIG_BLOCK, [])
    print(f"parent SIGINT blocked in mask: {signal.SIGINT in parent_mask}")

    run_dbt_process = subprocess.Popen(
        [
            "dbt",
            dbt_command,
            "--profiles-dir",
            project.profiles_dir,
            "--project-dir",
            project.project_root,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        env=os.environ.copy(),
        preexec_fn=_unblock_sigint_in_child,
    )

    # Drain the child's output on a background thread. Reading in the main
    # thread with a blocking readline() means that if the "START" line is never
    # delivered the test hangs until the CI job timeout; draining here keeps the
    # pipe from filling and lets the main thread enforce its own timeouts.
    captured = []
    started = threading.Event()

    def _drain():
        for raw in run_dbt_process.stdout:
            line = raw.decode("utf-8")
            captured.append(line)
            if "1 of 1 START" in line:
                started.set()

    drainer = threading.Thread(target=_drain, daemon=True)
    drainer.start()

    # Interrupt once the model has started (preferred) or after a bounded
    # fallback, so cancellation is always exercised and never hangs.
    started.wait(timeout=_START_TIMEOUT)
    time.sleep(1)

    # Resend SIGINT until dbt exits. A single process-directed SIGINT can fail
    # to trigger cancellation (it may land before the main thread is parked in
    # queue.join(), or be handled on a thread that does not raise
    # KeyboardInterrupt); resending makes cancellation reliable, while the hard
    # deadline guarantees we fail fast instead of hanging until the CI timeout.
    deadline = time.monotonic() + _EXIT_TIMEOUT
    exited = False
    while time.monotonic() < deadline:
        run_dbt_process.send_signal(signal.SIGINT)
        try:
            run_dbt_process.wait(timeout=_SIGINT_INTERVAL)
            exited = True
            break
        except subprocess.TimeoutExpired:
            continue

    if not exited:
        run_dbt_process.kill()
        run_dbt_process.wait()
        drainer.join(timeout=5)
        raise AssertionError(
            f"dbt did not exit within {_EXIT_TIMEOUT}s of repeated SIGINT; "
            "cancellation appears to have hung.\n" + "".join(captured)
        )

    drainer.join(timeout=5)
    return "".join(captured)


def _get_job_id(project, table_name):
    # Because we run this in a subprocess we have to actually call Bigquery and look up the job id
    with get_connection(project.adapter):
        job_id = project.run_sql(
            _get_info_schema_jobs_query(project.database, project.test_schema, table_name)
        )

    return job_id


@pytest.mark.skipif(
    platform.system() == "Windows", reason="running signt is unsupported on Windows."
)
class TestBigqueryCancelsQueriesOnKeyboardInterrupt:
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "model.sql": _LONG_RUNNING_MODEL_SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {
            "seed.csv": _SEED_CSV,
        }

    def test_bigquery_cancels_queries_for_model_on_keyboard_interrupt(self, project):
        std_out_log = _run_dbt_in_subprocess(project, "run")

        assert "CANCEL query model.test.model" in std_out_log
        assert len(_get_job_id(project, "model")) == 1

    @pytest.mark.skip(reason="cannot reliably cancel seed queries in time")
    def test_bigquery_cancels_queries_for_seed_on_keyboard_interrupt(self, project):
        std_out_log = _run_dbt_in_subprocess(project, "seed")

        assert "CANCEL query seed.test.seed" in std_out_log
        # we can't assert the job id since we can't kill the seed process fast enough to cancel it
