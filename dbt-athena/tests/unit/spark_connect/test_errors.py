"""Tests for Spark Connect transient-error classification."""

import pytest

from dbt.adapters.athena.spark_connect.errors import (
    TRANSIENT_GRPC_STATUS_CODES,
    TRANSIENT_SPARK_PATTERNS,
    is_transient_spark_error,
)


class _FakeGrpcCode:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeGrpcError(Exception):
    def __init__(self, message: str, code_name: str) -> None:
        super().__init__(message)
        self._code = _FakeGrpcCode(code_name)

    def code(self) -> _FakeGrpcCode:
        return self._code


class _FakeGrpcCallableErrorRaises(Exception):
    """A non-gRPC class that exposes a code() callable which raises."""

    def code(self):
        raise RuntimeError("not actually a gRPC error")


@pytest.mark.parametrize("pattern", TRANSIENT_SPARK_PATTERNS)
def test_pattern_match_is_transient(pattern):
    err = Exception(f"some prefix: {pattern} ; some suffix")
    assert is_transient_spark_error(err) is True


def test_unknown_message_is_not_transient():
    assert is_transient_spark_error(Exception("some unrelated failure")) is False


@pytest.mark.parametrize("code_name", sorted(TRANSIENT_GRPC_STATUS_CODES))
def test_grpc_status_code_is_transient(code_name):
    assert is_transient_spark_error(_FakeGrpcError("anything", code_name)) is True


def test_non_transient_grpc_code_is_not_transient():
    assert is_transient_spark_error(_FakeGrpcError("boom", "INVALID_ARGUMENT")) is False


def test_grpc_transient_via_cause_chain():
    inner = _FakeGrpcError("rpc unavailable", "UNAVAILABLE")
    try:
        raise RuntimeError("wrapper") from inner
    except RuntimeError as e:
        assert is_transient_spark_error(e) is True


def test_grpc_transient_via_context_chain():
    try:
        try:
            raise _FakeGrpcError("rpc aborted", "ABORTED")
        except _FakeGrpcError:
            raise RuntimeError("wrapper")
    except RuntimeError as e:
        assert is_transient_spark_error(e) is True


def test_deep_cause_chain_walked():
    deepest = _FakeGrpcError("resource exhausted", "RESOURCE_EXHAUSTED")
    middle = RuntimeError("middle")
    middle.__cause__ = deepest
    outer = RuntimeError("outer")
    outer.__cause__ = middle
    assert is_transient_spark_error(outer) is True


def test_code_callable_that_raises_does_not_break_classification():
    # The class lies about being a gRPC error; classifier should fall back
    # to string matching and still return False (no transient pattern).
    assert is_transient_spark_error(_FakeGrpcCallableErrorRaises("nope")) is False


def test_code_callable_that_raises_with_transient_message_still_matches_string():
    err = _FakeGrpcCallableErrorRaises("Pool not running")
    assert is_transient_spark_error(err) is True


def test_non_callable_code_attribute_is_ignored():
    err = Exception("benign")
    err.code = "UNAVAILABLE"  # attribute, not method
    assert is_transient_spark_error(err) is False
