"""Connection-manager behaviour that needs no database.

Covers the three things in ``connections.py`` that are easy to break by "simplifying":
query-comment suppression, the no-op transaction methods, and the purity of
``get_response``.
"""

import threading
from multiprocessing import get_context
from types import SimpleNamespace

import pytest

from adbc_driver_manager import AdbcStatusCode, OperationalError
from dbt.adapters.contracts.connection import Connection, ConnectionState, Identifier
from dbt_common.events import get_event_manager
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.lakecompute import connections as connections_module
from dbt.adapters.lakecompute.connections import (
    LakeComputeAdapterResponse,
    LakeComputeConnectionManager,
    _ATTR_ACTIVE_STATEMENT,
    _ATTR_QUERY_ID,
    _ATTR_WARNINGS,
    _get_or_create_shared_database,
)
from dbt.adapters.lakecompute.credentials import LakeComputeCredentials
from dbt.adapters.lakecompute.driver import ResolvedDriver


def make_credentials(**overrides) -> LakeComputeCredentials:
    kwargs = {"database": "db", "schema": "sch"}
    kwargs.update(overrides)
    return LakeComputeCredentials(**kwargs)  # type: ignore[arg-type]


def make_manager(**credential_overrides) -> LakeComputeConnectionManager:
    credentials = make_credentials(**credential_overrides)
    profile = SimpleNamespace(
        credentials=credentials,
        project_name="my_project",
        # dbt wraps this in /* */ itself and rejects a comment that already contains "*/"
        query_comment=SimpleNamespace(comment="dbt", append=False),
    )
    return LakeComputeConnectionManager(profile, get_context("spawn"))  # type: ignore[arg-type]


def attach_connection(manager: LakeComputeConnectionManager) -> Connection:
    connection = Connection(
        type=Identifier("lakecompute"),
        name="test",
        state=ConnectionState.OPEN,  # type: ignore[arg-type]
        transaction_open=False,
        handle=None,
        credentials=manager.profile.credentials,
    )
    manager.set_thread_connection(connection)
    return connection


class FakeCursor:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount


class FakeStatement:
    """Stands in for an AdbcStatement: records cancels, rejects nothing."""

    def __init__(self):
        self.cancels = 0

    def cancel(self):
        self.cancels += 1

    def set_options(self, **options):
        pass

    def get_option(self, option):
        raise KeyError(option)


class FakeAdbcCursor:
    """Enough of ``adbc_driver_manager.dbapi.Cursor`` for the execute paths."""

    def __init__(self, rows=((1,),), description=(("n", 0),), on_execute=None, on_fetch=None):
        self.adbc_statement = FakeStatement()
        self.rowcount = len(rows)
        self.closed = False
        self.calls = []
        self._rows = list(rows)
        self._description = description
        self._on_execute = on_execute
        self._on_fetch = on_fetch
        self.description = None

    def _ran(self, kind):
        self.calls.append(kind)
        if self._on_execute is not None:
            self._on_execute()

    def execute(self, sql, parameters=None):
        self._ran("execute")
        # only the fetching primitive produces a result set
        self.description = self._description

    def executemany(self, sql, seq_of_parameters):
        self._ran("executemany")
        self.description = None

    def fetchall(self):
        if self._on_fetch is not None:
            self._on_fetch()
        return list(self._rows)

    def fetchmany(self, size):
        if self._on_fetch is not None:
            self._on_fetch()
        return list(self._rows[:size])

    def close(self):
        self.closed = True


class FakeHandle:
    """Stands in for ``dbapi.Connection``."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.connection_cancels = 0

    def cursor(self):
        return self._cursor

    def close(self):
        # ADBC 1.9 refuses connection cleanup while a cursor's statement is open.
        if not self._cursor.closed:
            raise RuntimeError("Cannot close AdbcConnection with open AdbcStatement")

    def adbc_cancel(self):
        # connection-level cancel is NOT_IMPLEMENTED on the pinned driver
        self.connection_cancels += 1
        raise AssertionError("cancel() must not use connection-level cancel")


def prepared(manager, cursor):
    """Attach an open connection carrying ``cursor`` and silence the query comment."""
    connection = attach_connection(manager)
    connection.handle = FakeHandle(cursor)
    manager.set_query_header({})
    return connection


@pytest.fixture
def query_events():
    events = []
    event_manager = get_event_manager()
    event_manager.callbacks.append(events.append)
    try:
        yield events
    finally:
        event_manager.callbacks.remove(events.append)


class TestQueryComment:
    def test_suppressed_by_default(self):
        """Off unless the profile opts in."""
        manager = make_manager()
        manager.set_query_header({})
        assert manager.query_header is None
        assert manager._add_query_comment("select 1") == "select 1"

    def test_opt_in_restores_it(self):
        manager = make_manager(query_comment_enabled=True)
        manager.set_query_header({})
        assert manager.query_header is not None


class TestTransactions:
    """Autocommit is structural: begin/commit are no-ops and never open a transaction."""

    def test_begin_and_commit_never_open_a_transaction(self, query_events):
        # the base class raises DbtInternalError on a double begin or a bare commit
        manager = make_manager()
        connection = attach_connection(manager)
        manager.begin()
        manager.begin()
        manager.commit()
        manager.commit()
        assert connection.transaction_open is False
        assert query_events == []

    def test_clear_transaction_and_rollback_do_nothing(self, query_events):
        manager = make_manager()
        attach_connection(manager)
        manager.clear_transaction()
        manager.rollback_if_open()
        assert query_events == []


class TestGetResponse:
    def test_shape(self):
        response = LakeComputeConnectionManager.get_response(FakeCursor(rowcount=7))
        assert isinstance(response, LakeComputeAdapterResponse)
        assert response.code == "SUCCESS"
        assert response._message == "SUCCESS 7"
        assert response.rows_affected == 7

    def test_unknown_rowcount_is_reported_as_none(self):
        response = LakeComputeConnectionManager.get_response(FakeCursor(rowcount=-1))
        assert response.rows_affected is None
        assert response._message == "SUCCESS"

    def test_warnings_are_appended(self):
        cursor = FakeCursor(rowcount=1)
        setattr(cursor, _ATTR_WARNINGS, "partition pruning disabled")
        response = LakeComputeConnectionManager.get_response(cursor)
        assert response.warning == "partition pruning disabled"
        assert "partition pruning disabled" in response._message

    def test_query_id_is_carried_through(self):
        cursor = FakeCursor()
        setattr(cursor, _ATTR_QUERY_ID, "q-123")
        assert LakeComputeConnectionManager.get_response(cursor).query_id == "q-123"

    def test_is_pure(self):
        """Called twice per statement -- once in add_query, once in execute."""
        cursor = FakeCursor(rowcount=3)
        setattr(cursor, _ATTR_WARNINGS, "w")
        first = LakeComputeConnectionManager.get_response(cursor)
        second = LakeComputeConnectionManager.get_response(cursor)
        assert first == second


class TestStatementOptions:
    def test_includes_project(self):
        manager = make_manager()
        options = manager._statement_options()
        assert options["adbc.dbt.project"] == "my_project"
        # model is absent outside a node context (on-run-start, dbt debug)
        assert "adbc.dbt.model" not in options

    def test_all_keys_are_namespaced(self):
        manager = make_manager()
        assert all(key.startswith("adbc.dbt.") for key in manager._statement_options())


class TestAddQuery:
    def test_bindings_are_rejected_with_a_clear_message(self):
        manager = make_manager()
        attach_connection(manager)
        with pytest.raises(DbtRuntimeError, match="does not support parameter binding"):
            manager.add_query("insert into t values (?)", bindings=[(1,)])

    def test_fetch_is_keyword_only(self):
        """Keeps SQLAdapter.add_query and every macro-level add_query call working."""
        import inspect

        params = inspect.signature(LakeComputeConnectionManager.add_query).parameters
        assert params["fetch"].kind is inspect.Parameter.KEYWORD_ONLY
        assert params["fetch"].default is False


class TestFetchForwarding:
    """`SQLConnectionManager.execute` never forwards `fetch`, so both of these are overridden.

    Without them a fetching query lands on `executemany` -> `execute_update`, which produces
    no result set at all: `dbt show`, `run_query` and metadata queries come back silently
    empty instead of failing.
    """

    def test_execute_with_fetch_uses_the_fetching_primitive(self):
        manager = make_manager()
        cursor = FakeAdbcCursor(rows=((1,), (2,)))
        prepared(manager, cursor)

        response, table = manager.execute("select n from t", fetch=True)

        assert cursor.calls == ["execute"]
        assert len(table.rows) == 2
        assert response.code == "SUCCESS"

    def test_execute_without_fetch_uses_execute_update(self):
        manager = make_manager()
        cursor = FakeAdbcCursor()
        prepared(manager, cursor)

        _, table = manager.execute("create table t as select 1")

        assert cursor.calls == ["executemany"]
        assert len(table.rows) == 0

    def test_execute_honours_limit(self):
        manager = make_manager()
        cursor = FakeAdbcCursor(rows=((1,), (2,), (3,)))
        prepared(manager, cursor)

        _, table = manager.execute("select n from t", fetch=True, limit=2)
        assert len(table.rows) == 2

    def test_execute_closes_the_cursor(self):
        # each cursor owns an AdbcStatement; with one shared database per run nothing else
        # would release them until GC
        manager = make_manager()
        cursor = FakeAdbcCursor()
        prepared(manager, cursor)

        manager.execute("select 1", fetch=True)
        assert cursor.closed is True

    def test_add_select_query_asks_for_results(self):
        # BaseAdapter.get_column_schema_from_query reads cursor.description off this
        manager = make_manager()
        cursor = FakeAdbcCursor()
        prepared(manager, cursor)

        _, returned = manager.add_select_query("select n from t")

        assert returned.calls == ["execute"]
        assert returned.description is not None
        # the base contract hands the cursor back open
        assert returned.closed is False


class TestQueryFailures:
    @pytest.mark.parametrize(
        "method,kwargs",
        [
            ("execute", {"fetch": True}),
            ("execute", {"fetch": False}),
            ("add_query", {"fetch": True}),
            ("add_query", {"fetch": False}),
            ("add_select_query", {}),
        ],
    )
    def test_execution_failure_closes_cursor_before_connection_cleanup(
        self, method, kwargs, query_events
    ):
        def fail():
            raise OperationalError("invalid SQL", status_code=AdbcStatusCode.INVALID_ARGUMENT)

        manager = make_manager()
        cursor = FakeAdbcCursor(on_execute=fail)
        connection = prepared(manager, cursor)

        with pytest.raises(DbtDatabaseError, match="invalid SQL") as error:
            getattr(manager, method)("select invalid", **kwargs)

        assert cursor.closed
        assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is None
        # Keep the exception and its traceback alive, as dbt does during release.
        assert error.value.__traceback__ is not None
        manager.release()
        assert connection.state == ConnectionState.CLOSED
        assert not any(e.info.name == "SQLQueryStatus" for e in query_events)

    @pytest.mark.parametrize("limit", [None, 1])
    @pytest.mark.parametrize(
        "failure",
        [
            OSError("result stream failed"),
            OperationalError("result stream failed", status_code=AdbcStatusCode.TIMEOUT),
        ],
    )
    def test_fetch_failure_is_translated_and_closes_cursor(self, limit, failure, query_events):
        def fail():
            raise failure

        manager = make_manager()
        cursor = FakeAdbcCursor(on_fetch=fail)
        connection = prepared(manager, cursor)

        with pytest.raises(DbtDatabaseError, match="result stream failed") as error:
            manager.execute("select n from t", fetch=True, limit=limit)

        if isinstance(failure, OperationalError):
            assert "timeout_seconds" in str(error.value)
        assert cursor.closed
        assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is None
        manager.release()
        assert not any(e.info.name == "SQLQueryStatus" for e in query_events)

    def test_cursor_close_failure_does_not_mask_query_error(self):
        def fail_query():
            raise OperationalError("original SQL error", status_code=AdbcStatusCode.INTERNAL)

        def fail_close():
            raise RuntimeError("cleanup error")

        manager = make_manager()
        cursor = FakeAdbcCursor(on_execute=fail_query)
        prepared(manager, cursor)
        cursor.close = fail_close

        with pytest.raises(DbtDatabaseError, match="original SQL error"):
            manager.execute("select invalid", fetch=True)


class TestQueryEvents:
    @pytest.mark.parametrize("method", ["execute", "add_query"])
    @pytest.mark.parametrize("fetch", [False, True])
    def test_emits_structured_query_events(self, method, fetch, query_events):
        manager = make_manager()
        cursor = FakeAdbcCursor()
        prepared(manager, cursor)
        options = {
            "adbc.dbt.last_query_id": "query-123",
            "adbc.dbt.last_warnings": "query warning",
        }
        cursor.adbc_statement.get_option = options.get

        getattr(manager, method)("select 1", fetch=fetch)
        cursor.close()

        assert [e.info.name for e in query_events] == [
            "ConnectionUsed",
            "SQLQuery",
            "SQLQueryStatus",
        ]
        used, query, status = [e.data for e in query_events]
        assert used.conn_type == "lakecompute"
        assert used.conn_name == "test"
        assert query.conn_name == "test"
        assert query.sql == "select 1"
        assert status.status == "SUCCESS 1 [query warning]"
        assert status.query_id == "query-123"
        assert status.elapsed >= 0

    def test_success_is_logged_after_fetching_finishes(self, query_events):
        def during_fetch():
            assert [e.info.name for e in query_events] == ["ConnectionUsed", "SQLQuery"]

        manager = make_manager()
        prepared(manager, FakeAdbcCursor(on_fetch=during_fetch))
        manager.execute("select 1", fetch=True)

        assert query_events[-1].info.name == "SQLQueryStatus"

    def test_abridged_sql_is_used_in_the_query_event(self, query_events):
        manager = make_manager()
        cursor = FakeAdbcCursor()
        prepared(manager, cursor)
        sql = "select '" + "x" * 600 + "'"

        manager.add_query(sql, abridge_sql_log=True)
        cursor.close()

        query = next(e.data for e in query_events if e.info.name == "SQLQuery")
        assert query.sql == sql[:512] + "..."


class TestCancel:
    @pytest.mark.parametrize("limit", [None, 1])
    def test_fetch_remains_cancellable_from_another_thread(self, limit):
        manager = make_manager()
        cursor = FakeAdbcCursor()
        connection = prepared(manager, cursor)

        def cancel_during_fetch():
            assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is cursor.adbc_statement
            canceller = threading.Thread(target=manager.cancel, args=(connection,))
            canceller.start()
            canceller.join(timeout=5)
            assert not canceller.is_alive()
            assert cursor.adbc_statement.cancels == 1
            assert not cursor.closed

        cursor._on_fetch = cancel_during_fetch
        manager.execute("select n from t", fetch=True, limit=limit)

        assert cursor.closed
        assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is None
        manager.cancel(connection)
        assert cursor.adbc_statement.cancels == 1

    def test_cancels_the_in_flight_statement_not_the_connection(self):
        manager = make_manager(token="tok")
        cursor = FakeAdbcCursor()
        connection = prepared(manager, cursor)

        # cancel arrives from another thread while the query is in the driver
        cursor._on_execute = lambda: manager.cancel(connection)
        manager.add_query("select pg_sleep(60)")

        assert cursor.adbc_statement.cancels == 1
        assert connection.handle.connection_cancels == 0

    def test_statements_are_deregistered_once_the_query_returns(self):
        manager = make_manager(token="tok")
        cursor = FakeAdbcCursor()
        connection = prepared(manager, cursor)

        manager.add_query("select 1")

        assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is None
        # nothing in flight, so nothing to cancel
        manager.cancel(connection)
        assert cursor.adbc_statement.cancels == 0

    def test_cancel_is_safe_with_nothing_in_flight(self):
        manager = make_manager(token="tok")
        connection = prepared(manager, FakeAdbcCursor())
        manager.cancel(connection)

    def test_a_failing_cancel_does_not_propagate(self):
        # it must not mask the original failure that prompted the cancel
        manager = make_manager(token="tok")
        cursor = FakeAdbcCursor()
        connection = prepared(manager, cursor)

        def explode():
            raise RuntimeError("NOT_IMPLEMENTED")

        cursor.adbc_statement.cancel = explode
        cursor._on_execute = lambda: manager.cancel(connection)
        manager.add_query("select 1")

    @pytest.mark.parametrize("during_fetch", [False, True])
    def test_query_cannot_deregister_while_a_cancel_is_in_the_driver(self, during_fetch):
        # AdbcStatement.cancel takes no lock, so the owning thread must not be able to
        # deregister (and then release) the statement until the cancel has returned
        manager = make_manager(token="tok")
        cursor = FakeAdbcCursor()
        connection = prepared(manager, cursor)

        cancel_entered = threading.Event()
        release_cancel = threading.Event()
        query_returned = threading.Event()

        def slow_cancel():
            cancel_entered.set()
            release_cancel.wait(timeout=5)

        cursor.adbc_statement.cancel = slow_cancel

        def run_query():
            manager.set_thread_connection(connection)
            if during_fetch:
                manager.execute("select 1", fetch=True)
            else:
                manager.add_query("select 1")
            query_returned.set()

        def start_cancel():
            threading.Thread(target=manager.cancel, args=(connection,)).start()
            assert cancel_entered.wait(timeout=5)

        if during_fetch:
            cursor._on_fetch = start_cancel
        else:
            cursor._on_execute = start_cancel
        worker = threading.Thread(target=run_query)
        worker.start()

        assert cancel_entered.wait(timeout=5)
        assert not query_returned.wait(timeout=0.2)
        assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is cursor.adbc_statement
        assert not cursor.closed

        release_cancel.set()
        worker.join(timeout=5)
        assert query_returned.is_set()
        assert getattr(connection, _ATTR_ACTIVE_STATEMENT) is None
        assert cursor.closed is during_fetch


class TestOpen:
    def test_open_uses_the_shared_registry(self, monkeypatch):
        manager = make_manager(token="tok")
        connection = attach_connection(manager)
        resolved = ResolvedDriver(path="/lib/x.so", entrypoint="AdbcDriverDbtInit", db_kwargs={})
        monkeypatch.setattr(connections_module, "resolve_driver", lambda credentials: resolved)

        taken = []
        monkeypatch.setattr(
            connections_module,
            "_get_or_create_shared_database",
            lambda r: taken.append("shared") or object(),
        )
        monkeypatch.setattr(
            connections_module, "_new_thread_connection", lambda s: FakeHandle(FakeAdbcCursor())
        )

        connection.state = ConnectionState.INIT  # type: ignore[assignment]
        LakeComputeConnectionManager.open(connection)
        assert taken == ["shared"]


class TestSharedDatabaseRegistry:
    @pytest.fixture(autouse=True)
    def created(self, monkeypatch):
        """Record AdbcDatabase constructions without loading a real driver."""
        from adbc_driver_manager import _lib, dbapi

        created = []
        monkeypatch.setattr(_lib, "AdbcDatabase", lambda **kwargs: created.append(kwargs))
        monkeypatch.setattr(dbapi, "_SharedDatabase", lambda database: object())
        monkeypatch.setattr(connections_module, "_REGISTRY", {})
        return created

    def _resolved(self, **db_kwargs) -> ResolvedDriver:
        return ResolvedDriver(
            path="/lib/libadbc_driver_dbt.so",
            entrypoint="AdbcDriverDbtInit",
            db_kwargs=db_kwargs or {"uri": "https://endpoint/"},
        )

    def test_identical_drivers_share_a_database(self, created):
        a = _get_or_create_shared_database(self._resolved())
        b = _get_or_create_shared_database(self._resolved())
        assert a is b
        assert len(created) == 1

    def test_different_db_kwargs_do_not_collide(self, created):
        """Two targets in one process must not accidentally share a database."""
        a = _get_or_create_shared_database(self._resolved(uri="https://a/"))
        b = _get_or_create_shared_database(self._resolved(uri="https://b/"))
        assert a is not b
        assert len(created) == 2

    def test_kwarg_ordering_does_not_matter(self, created):
        a = _get_or_create_shared_database(self._resolved(x="1", y="2"))
        b = _get_or_create_shared_database(self._resolved(y="2", x="1"))
        assert a is b
        assert len(created) == 1
