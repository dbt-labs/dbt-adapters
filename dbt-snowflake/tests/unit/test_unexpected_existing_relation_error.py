import unittest
from unittest import mock

import snowflake.connector.errors
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.snowflake.connections import (
    UNEXPECTED_EXISTING_RELATION_HINT,
    SnowflakeConnectionManager,
    _is_unexpected_existing_relation_error,
)

ALREADY_EXISTS = "002002 (42710): SQL compilation error:\nObject 'DB.SCH.INCR' already exists."
QUERY_COMMENT = '/* {"app": "dbt", "node_id": "model.test.incr"} */\n'
PYTHON_ALREADY_EXISTS = (
    "100357 (P0000): Python Interpreter Error:\nTraceback (most recent call last):\n"
    '  File "_udf_code.py", line 86, in materialize\n'
    "    df.write.mode(\"errorifexists\").save_as_table('DB.SCH.py_incr', table_type='transient')\n"
    "snowflake.snowpark.exceptions.SnowparkSQLException: (1304): 002002 (42710): "
    "SQL compilation error:\nObject 'DB.SCH.PY_INCR' already exists.\n"
    " in function PY_INCR__DBT_SP with handler main"
)


class TestIsUnexpectedExistingRelationError(unittest.TestCase):
    def test_create_table_as_without_replace(self):
        for sql in (
            "create transient table DB.SCH.INCR as (select 1 as id)",
            "create table DB.SCH.INCR as (select 1 as id)",
            "create iceberg table DB.SCH.INCR external_volume = 'v' as (select 1 as id)",
            "create transient table DB.SCH.INCR (id integer not null) as (select 1 as id)",
            QUERY_COMMENT + "create transient table DB.SCH.INCR as (select 1 as id)",
            "\n  CREATE TRANSIENT TABLE DB.SCH.INCR\n    AS (\n select 1 as id\n )",
        ):
            with self.subTest(sql=sql):
                self.assertTrue(_is_unexpected_existing_relation_error(ALREADY_EXISTS, sql))

    def test_python_write_in_errorifexists_mode(self):
        # The traceback carries the write call, so no SQL is needed (e.g. a named procedure CALL).
        self.assertTrue(
            _is_unexpected_existing_relation_error(PYTHON_ALREADY_EXISTS, "call DB.SCH.p()")
        )

    def test_other_statements_are_not_matched(self):
        for sql in (
            "create or replace transient table DB.SCH.INCR as (select 1 as id)",
            "create transient table if not exists DB.SCH.INCR (id integer)",
            "create transient table DB.SCH.INCR (id integer)",
            "create view DB.SCH.INCR as (select 1 as id)",
            "create schema DB.SCH",
            None,
        ):
            with self.subTest(sql=sql):
                self.assertFalse(_is_unexpected_existing_relation_error(ALREADY_EXISTS, sql))

    def test_other_errors_are_not_matched(self):
        sql = "create transient table DB.SCH.INCR as (select 1 as id)"
        for msg in (
            "002003 (02000): SQL compilation error:\nObject 'DB.SCH.SRC' does not exist.",
            "100132 (P0000): JavaScript execution error: 'X' already exists in this scope",
        ):
            with self.subTest(msg=msg):
                self.assertFalse(_is_unexpected_existing_relation_error(msg, sql))


class TestExceptionHandlerHint(unittest.TestCase):
    def _raise_through_handler(self, msg, sql, errno, sqlstate):
        error = snowflake.connector.errors.ProgrammingError(
            msg=msg, errno=errno, sqlstate=sqlstate, sfqid="query-id"
        )
        with self.assertRaises(DbtDatabaseError) as raised:
            with SnowflakeConnectionManager.exception_handler(mock.Mock(), sql):
                raise error
        return raised.exception.msg

    def test_failed_create_without_replace_explains_how_to_proceed(self):
        message = self._raise_through_handler(
            "SQL compilation error:\nObject 'DB.SCH.INCR' already exists.",
            QUERY_COMMENT + "create transient table DB.SCH.INCR as (select 1 as id)",
            errno=2002,
            sqlstate="42710",
        )
        self.assertIn("Object 'DB.SCH.INCR' already exists.", message)
        self.assertIn(UNEXPECTED_EXISTING_RELATION_HINT, message)
        self.assertIn("snowflake_fail_on_unexpected_existing_relation", message)

    def test_other_database_errors_are_unchanged(self):
        msg = "SQL compilation error:\nObject 'DB.SCH.SRC' does not exist or not authorized."
        message = self._raise_through_handler(
            msg, "select * from DB.SCH.SRC", errno=2003, sqlstate="02000"
        )
        self.assertIn(msg, message)
        self.assertNotIn(UNEXPECTED_EXISTING_RELATION_HINT, message)
