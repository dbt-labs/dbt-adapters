from dbt.adapters.record import RecordReplayCursor


class MockCursor:
    @property
    def rowcount(self) -> int:
        return 42

    def execute(self, operation, parameters=None) -> None:
        pass

    @property
    def unexpected_prop(self) -> bool:
        return True

    def unexpected_func(self) -> int:
        return 1


class MockConnection:
    pass


def test_record_cursor():
    # Ensure that record/replay's cursor wrapper forwards all property accesses
    # and function calls to the wrapped cursor, even if they were not expected.
    # Also, verify that a RecordReplayIssue is logged if unexpected accesses or
    # function calls happen.
    recorded_cursor = RecordReplayCursor(MockCursor(), MockConnection())  # type: ignore

    # Test that an expected property works as designed
    assert recorded_cursor.rowcount == 42
    # Test that an expected function call works as designed
    recorded_cursor.execute("select 1")

    events = []

    # Mock event firing
    recorded_cursor._fire_event = events.append

    # Test that an unexpected property works, but fires a warning
    assert recorded_cursor.unexpected_prop is True
    assert len(events) == 1
    assert events[0].__class__.__name__ == "RecordReplayIssue"
    events.clear()

    # Test that an unexpected function works, but fires a warning
    assert recorded_cursor.unexpected_func() == 1
    assert len(events) == 1
    assert events[0].__class__.__name__ == "RecordReplayIssue"
