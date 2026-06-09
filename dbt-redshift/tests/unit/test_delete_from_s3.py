from contextlib import contextmanager  # noqa: F401
from multiprocessing import get_context
from unittest import TestCase, mock

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from tests.unit.utils import (
    config_from_parts_or_dicts,
    inject_adapter,
)

BASE_OUTPUT = {
    "type": "redshift",
    "dbname": "redshift",
    "user": "root",
    "host": "thishostshouldnotexist.test.us-east-1",
    "pass": "password",
    "port": 5439,
    "schema": "public",
}

PROJECT_CFG = {
    "name": "X",
    "version": "0.1",
    "profile": "test",
    "project-root": "/tmp/dbt/does-not-exist",
    "config-version": 2,
}


def make_adapter(extra_credentials=None):
    output = {**BASE_OUTPUT, **(extra_credentials or {})}
    profile_cfg = {"outputs": {"test": output}, "target": "test"}
    config = config_from_parts_or_dicts(PROJECT_CFG, profile_cfg)
    adapter = RedshiftAdapter(config, get_context("spawn"))
    inject_adapter(adapter, RedshiftPlugin)
    return adapter


class TestDeleteFromS3(TestCase):
    def _run(self, s3_path):
        """Call delete_from_s3 with a mocked boto3 session; return (bucket, prefix used)."""
        adapter = make_adapter()
        mock_bucket = mock.MagicMock()
        mock_s3 = mock.MagicMock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_session = mock.MagicMock()
        mock_session.resource.return_value = mock_s3
        with mock.patch.object(adapter, "_boto3_session", return_value=mock_session):
            adapter.delete_from_s3(s3_path)
        return mock_s3, mock_bucket

    def test_parses_bucket_and_scopes_prefix_with_trailing_slash(self):
        mock_s3, mock_bucket = self._run("s3://my-bucket/iceberg/_dbt/schema/orders")
        mock_s3.Bucket.assert_called_once_with("my-bucket")
        # prefix must end with "/" so it doesn't also match a sibling like ".../orders_x"
        mock_bucket.objects.filter.assert_called_once_with(Prefix="iceberg/_dbt/schema/orders/")
        mock_bucket.objects.filter.return_value.delete.assert_called_once()

    def test_keeps_existing_trailing_slash(self):
        _, mock_bucket = self._run("s3://my-bucket/prefix/")
        mock_bucket.objects.filter.assert_called_once_with(Prefix="prefix/")

    def test_noop_on_empty_path(self):
        adapter = make_adapter()
        with mock.patch.object(adapter, "_boto3_session") as session:
            adapter.delete_from_s3(None)
            adapter.delete_from_s3("")
            session.assert_not_called()
