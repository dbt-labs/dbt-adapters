"""Guard the private ``adbc_driver_manager.dbapi`` API the connection manager depends on.

``connections.py`` builds its own ``dbapi.Connection`` over a shared ``_SharedDatabase`` so it
can get database sharing *and* autocommit at once (``Connection.adbc_clone()`` hardcodes
``autocommit=False``). That is one private name in one place, and these tests exist so a
dependency bump fails in CI rather than at a customer.

If one of these fails: either pin ``adbc-driver-manager`` below the breaking version, or
rework ``_new_thread_connection`` and re-point these assertions.
"""

import inspect

import pytest

dbapi = pytest.importorskip("adbc_driver_manager.dbapi")


def test_shared_database_exists():
    assert hasattr(dbapi, "_SharedDatabase"), (
        "adbc_driver_manager.dbapi._SharedDatabase is gone; "
        "LakeComputeConnectionManager cannot share one AdbcDatabase across threads without it."
    )


def test_shared_database_refcounts():
    for method in ("clone", "close"):
        assert hasattr(dbapi._SharedDatabase, method), f"_SharedDatabase.{method} is gone"


def test_shared_database_exposes_db_attribute():
    # _new_thread_connection reads shared._db to build the AdbcConnection
    source = inspect.getsource(dbapi._SharedDatabase.__init__)
    assert "self._db" in source, "_SharedDatabase no longer stores the database as _db"


def test_connection_init_accepts_shared_database_and_autocommit():
    signature = inspect.signature(dbapi.Connection.__init__)
    params = signature.parameters

    assert "db" in params, "Connection.__init__ no longer takes `db`"
    assert "conn" in params, "Connection.__init__ no longer takes `conn`"
    assert "conn_kwargs" in params, "Connection.__init__ no longer takes `conn_kwargs`"

    assert "autocommit" in params, "Connection.__init__ no longer takes `autocommit`"
    assert params["autocommit"].kind is inspect.Parameter.KEYWORD_ONLY

    annotation = str(params["db"].annotation)
    assert "_SharedDatabase" in annotation, (
        "Connection.__init__ no longer accepts a _SharedDatabase for `db`; "
        f"annotation is now {annotation!r}"
    )


def test_connection_clones_shared_database_itself():
    """We pass the shared database directly and must not pre-clone.

    If this changes, ``_new_thread_connection`` leaks or under-counts references.
    """
    source = inspect.getsource(dbapi.Connection.__init__)
    assert "_SharedDatabase" in source and "clone()" in source, (
        "Connection.__init__ no longer clones a passed-in _SharedDatabase; "
        "_new_thread_connection must now manage the refcount itself."
    )


def test_autocommit_true_skips_set_autocommit():
    """Autocommit must stay structural.

    ``autocommit=True`` has to set ``_commit_supported = False`` *without* calling
    ``set_autocommit``. If it ever starts issuing a real call, every thread connection would
    open a transaction that nothing commits.
    """
    source = inspect.getsource(dbapi.Connection.__init__)
    autocommit_branch = source.split("if autocommit:", 1)
    assert len(autocommit_branch) == 2, "Connection.__init__ no longer branches on autocommit"
    branch = autocommit_branch[1].split("else:", 1)[0]
    assert "_commit_supported" in branch
    assert "set_autocommit" not in branch


def test_cursor_exposes_statement_and_executemany():
    assert hasattr(dbapi.Cursor, "executemany"), "Cursor.executemany is gone"
    assert isinstance(dbapi.Cursor.adbc_statement, property)


def test_executemany_accepts_none_parameters():
    """``executemany(sql, None)`` is our no-fetch primitive.

    It must still reach ``execute_update`` (which sets rowcount and stays cancellable) rather
    than returning early the way an empty sequence does.
    """
    source = inspect.getsource(dbapi.Cursor.executemany)
    assert "execute_update" in source, "executemany no longer calls execute_update"
    assert "seq_of_parameters is not None" in source, (
        "executemany no longer distinguishes None from an empty sequence; "
        "passing None may now be a no-op."
    )
