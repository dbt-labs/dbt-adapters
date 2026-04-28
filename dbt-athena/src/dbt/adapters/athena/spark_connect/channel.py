"""Public API for the Athena Spark Connect channel builder."""

from __future__ import annotations

from typing import Any, Optional

_TOKEN_REFRESH_MARGIN_SECONDS = 120


def create_athena_channel_builder(
    athena_client: Any,
    session_id: str,
    endpoint_url: str,
    initial_auth_token: Optional[str] = None,
    initial_token_expiry: Any = None,
) -> Any:
    """Build a ChannelBuilder that auto-refreshes the Athena AuthToken."""
    from dbt.adapters.athena.spark_connect._channel_impl import AthenaChannelBuilder

    return AthenaChannelBuilder(
        athena_client,
        session_id,
        endpoint_url,
        initial_auth_token,
        initial_token_expiry,
    )
