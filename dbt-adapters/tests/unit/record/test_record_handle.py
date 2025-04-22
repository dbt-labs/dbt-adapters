from dbt.adapters.record import RecordReplayHandle


class MockConnection:
    pass


class MockHandle:
    @property
    def closed(self) -> bool:
        return False

    def rollback(self) -> None:
        pass

    @property
    def unexpected_prop(self) -> bool:
        return True

    def unexpected_func(self) -> int:
        return 1


def test_record_handle():
    # Ensure that record/replay's handle wrapper forwards all property accesses
    # and function calls to the wrapped cursor, even if they were not expected.
    # Also, verify that a RecordReplayIssue is logged if unexpected accesses or
    # function calls happen.
    recorded_handle = RecordReplayHandle(MockHandle(), MockConnection())  # type: ignore

    # Test that an expected property works as designed
    assert recorded_handle.closed is False
    # Test that an expected function call works as designed
    recorded_handle.rollback()

    events = []

    # Mock event firing
    recorded_handle._fire_event = events.append

    # Test that an unexpected property works, but fires a warning
    assert recorded_handle.unexpected_prop is True
    assert len(events) == 1
    assert events[0].__class__.__name__ == "RecordReplayIssue"
    events.clear()

    # Test that an unexpected function works, but fires a warning
    assert recorded_handle.unexpected_func() == 1
    assert len(events) == 1
    assert events[0].__class__.__name__ == "RecordReplayIssue"
