import pytest
from unittest.mock import MagicMock


def _fake_relation(project, name="DUMMY_DT"):
    """Return a bare-bones object with the three FQN fields."""
    rel = MagicMock()
    rel.identifier = name
    rel.database = project.database
    rel.schema = project.test_schema
    return rel


def _get_qiic_value(adapter):
    """
    Snowflake always yields one row; default is 'false'.
    """
    _, tbl = adapter.execute(
        "SHOW PARAMETERS LIKE 'QUOTED_IDENTIFIERS_IGNORE_CASE' IN SESSION",
        fetch=True,
    )
    # tbl.rows[0] is guaranteed by SHOW … IN SESSION
    val = tbl.rows[0][1]
    return val.strip().lower() if val else None  # e.g. 'true' or 'false'


class TestQuotedIdentifiersFlag:
    """
    Ensure describe_dynamic_table() restores the session parameter
    QUOTED_IDENTIFIERS_IGNORE_CASE to its original value.
    """

    @pytest.mark.parametrize("initial_setting", ["true", "false"])
    def test_restores_parameter(self, project, initial_setting):
        adapter = project.adapter

        adapter.execute(
            f"ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = {initial_setting.upper()}",
            fetch=False,
        )
        before = _get_qiic_value(adapter)
        assert before == initial_setting

        relation = _fake_relation(project)
        adapter.describe_dynamic_table(relation)

        after = _get_qiic_value(adapter)
        assert after == before, f"Parameter not restored: before={before!r}, after={after!r}"


class TestFlagRestoredOnSqlError:
    """
    Simulate adapter.execute raising an exception *after* the flag
    has been flipped, and assert the flag is still reset.
    """

    @pytest.mark.parametrize("initial_setting", ["true", "false"])
    def test_restored_on_exception(self, project, monkeypatch, initial_setting):
        adapter = project.adapter

        # initial flag state
        adapter.execute(
            f"ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = {initial_setting.upper()}",
            fetch=False,
        )
        before = _get_qiic_value(adapter)

        # monkey-patch execute so the 5th call raises
        call_count = {"n": 0}

        def flaky_execute(sql, fetch=False):
            call_count["n"] += 1
            if call_count["n"] == 5:
                raise RuntimeError("boom")
            return adapter.__class__.execute(adapter, sql, fetch=fetch)

        monkeypatch.setattr(adapter, "execute", flaky_execute)

        relation = _fake_relation(project)
        with pytest.raises(RuntimeError, match="boom"):
            adapter.describe_dynamic_table(relation)

        after = _get_qiic_value(adapter)
        assert after == before
