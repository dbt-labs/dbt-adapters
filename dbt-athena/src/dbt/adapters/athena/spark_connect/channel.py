"""Public API for the Athena Spark Connect channel builder.

The pyspark-dependent ``AthenaChannelBuilder`` class lives in
``_channel_impl`` and is imported lazily from
``create_athena_channel_builder`` so that this module stays importable
without the optional Spark Connect dependency installed.
"""

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
    """Build a ChannelBuilder that auto-refreshes the Athena AuthToken.

    The function-local import defers pyspark loading until Spark Connect
    is actually used; Python's import system handles single-execution
    and thread safety via the import lock and ``sys.modules`` cache.
    """
    from dbt.adapters.athena.spark_connect._channel_impl import AthenaChannelBuilder

    return AthenaChannelBuilder(
        athena_client,
        session_id,
        endpoint_url,
        initial_auth_token,
        initial_token_expiry,
    )
