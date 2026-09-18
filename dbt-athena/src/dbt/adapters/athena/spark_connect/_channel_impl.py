"""pyspark-dependent ``AthenaChannelBuilder`` definition.

Imported lazily from ``channel.create_athena_channel_builder`` so the
public ``channel`` module stays importable without pyspark installed.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from dbt_common.exceptions import DbtRuntimeError
from mypy_boto3_athena.client import AthenaClient
from pyspark.sql.connect.client.core import ChannelBuilder

from dbt.adapters.athena.spark_connect.channel import _TOKEN_REFRESH_MARGIN_SECONDS
from dbt.adapters.athena.spark_connect.pyspark_patches import apply_pyspark_workarounds

apply_pyspark_workarounds()


class AthenaChannelBuilder(ChannelBuilder):
    """ChannelBuilder that refreshes the Athena AuthToken before expiry."""

    def __init__(
        self,
        client: AthenaClient,
        sid: str,
        url: str,
        auth_token: Optional[str],
        token_expiry: Optional[datetime],
    ) -> None:
        sc_url = url.replace("https://", "sc://", 1) + ":443/;use_ssl=true"
        super().__init__(sc_url)
        self._athena_client = client
        self._athena_session_id = sid
        self._auth_token = auth_token
        self._token_expiry = token_expiry
        # pyspark invokes metadata() from concurrent gRPC executor threads.
        self._token_lock = threading.Lock()

    def _refresh_token(self) -> None:
        if self._token_is_fresh():
            return
        with self._token_lock:
            if self._token_is_fresh():
                return
            response = self._athena_client.get_session_endpoint(SessionId=self._athena_session_id)
            auth_token = response.get("AuthToken")
            if not auth_token:
                raise DbtRuntimeError(
                    f"GetSessionEndpoint returned no AuthToken for session "
                    f"{self._athena_session_id}"
                )
            self._auth_token = auth_token
            self._token_expiry = response.get("AuthTokenExpirationTime")

    def _token_is_fresh(self) -> bool:
        if not (self._auth_token and self._token_expiry):
            return False
        remaining = (self._token_expiry - datetime.now(timezone.utc)).total_seconds()
        return remaining > _TOKEN_REFRESH_MARGIN_SECONDS

    def metadata(self) -> List[Tuple[str, str]]:
        self._refresh_token()
        with self._token_lock:
            token = self._auth_token
            base = [(k, v) for k, v in super().metadata() if k != "x-aws-proxy-auth"]
            base.append(("x-aws-proxy-auth", token))
            return base
