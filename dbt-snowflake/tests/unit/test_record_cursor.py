from dbt.adapters.snowflake.connections import SnowflakeConnectionManager
from dbt.adapters.snowflake.record.cursor.cursor import SnowflakeRecordReplayCursor


class MockStats:
    """Mock object that mimics snowflake-connector-python's QueryResultStats."""

    def __init__(
        self,
        num_rows_inserted=100,
        num_rows_deleted=10,
        num_rows_updated=5,
        num_dml_duplicates=2,
    ):
        self.num_rows_inserted = num_rows_inserted
        self.num_rows_deleted = num_rows_deleted
        self.num_rows_updated = num_rows_updated
        self.num_dml_duplicates = num_dml_duplicates


class MockCursor:
    """Mock cursor that mimics snowflake-connector-python's SnowflakeCursor."""

    def __init__(self, stats=None):
        self._stats = stats

    @property
    def rowcount(self) -> int:
        return 42

    @property
    def sqlstate(self) -> str:
        return "00000"

    @property
    def sfqid(self) -> str:
        return "01abc123-0001-abcd-0000-00012345abcd"

    @property
    def stats(self):
        return self._stats

    def execute(self, operation, parameters=None) -> None:
        pass

    @property
    def unexpected_prop(self) -> bool:
        return True

    def unexpected_func(self) -> int:
        return 1


class MockConnection:
    name = "test_connection"


def test_snowflake_record_cursor_sqlstate():
    """Test that the sqlstate property works correctly."""
    recorded_cursor = SnowflakeRecordReplayCursor(MockCursor(), MockConnection())  # type: ignore
    assert recorded_cursor.sqlstate == "00000"


def test_snowflake_record_cursor_sfqid():
    """Test that the sfqid property works correctly."""
    recorded_cursor = SnowflakeRecordReplayCursor(MockCursor(), MockConnection())  # type: ignore
    assert recorded_cursor.sfqid == "01abc123-0001-abcd-0000-00012345abcd"


def test_snowflake_record_cursor_stats():
    """Test that the stats property works correctly."""
    mock_stats = MockStats()
    recorded_cursor = SnowflakeRecordReplayCursor(
        MockCursor(stats=mock_stats), MockConnection()
    )  # type: ignore

    stats = recorded_cursor.stats
    assert stats.num_rows_inserted == 100
    assert stats.num_rows_deleted == 10
    assert stats.num_rows_updated == 5
    assert stats.num_dml_duplicates == 2


def test_snowflake_record_cursor_stats_none():
    """Test that the stats property handles None correctly."""
    recorded_cursor = SnowflakeRecordReplayCursor(
        MockCursor(stats=None), MockConnection()
    )  # type: ignore

    assert recorded_cursor.stats is None


def test_snowflake_record_cursor_inherited_properties():
    """Test that inherited properties from RecordReplayCursor work correctly."""
    recorded_cursor = SnowflakeRecordReplayCursor(MockCursor(), MockConnection())  # type: ignore

    # Test inherited rowcount property
    assert recorded_cursor.rowcount == 42

    # Test inherited execute method
    recorded_cursor.execute("SELECT 1")


def test_snowflake_record_cursor_unexpected_access():
    """Test that unexpected property/method access fires a warning but still works."""
    recorded_cursor = SnowflakeRecordReplayCursor(MockCursor(), MockConnection())  # type: ignore

    events = []
    # Mock event firing
    recorded_cursor._fire_event = events.append

    # Test that an unexpected property works, but fires a warning
    assert recorded_cursor.unexpected_prop is True
    assert len(events) == 1
    assert events[0].__class__.__name__ == "RecordReplayIssue"
    assert "unexpected_prop" in events[0].msg
    events.clear()

    # Test that an unexpected function works, but fires a warning
    assert recorded_cursor.unexpected_func() == 1
    assert len(events) == 1
    assert events[0].__class__.__name__ == "RecordReplayIssue"
    assert "unexpected_func" in events[0].msg


def test_get_response_no_unexpected_access_warnings():
    """Ensure get_response() doesn't trigger any unexpected attribute access warnings.

    This is a regression test. If new cursor attributes are accessed in get_response()
    without being added to SnowflakeRecordReplayCursor, this test will fail.
    """
    events = []

    # Test with stats present
    mock_cursor = MockCursor(stats=MockStats())
    recorded_cursor = SnowflakeRecordReplayCursor(mock_cursor, MockConnection())  # type: ignore
    recorded_cursor._fire_event = events.append

    # Call get_response - this is the actual code path
    response = SnowflakeConnectionManager.get_response(recorded_cursor)

    # Verify no unexpected access warnings were fired
    assert len(events) == 0, (
        f"Unexpected attribute access in get_response(): {[e.msg for e in events]}. "
        "Add the missing attribute(s) to SnowflakeRecordReplayCursor."
    )

    # Verify the response was created successfully
    assert response is not None
    assert response.code == "00000"  # SQL success state from mock cursor
