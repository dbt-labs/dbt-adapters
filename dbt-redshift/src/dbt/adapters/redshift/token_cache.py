"""
Token caching utilities for Azure SSO authentication.
"""

import base64
import json
import stat
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional


class TokenCache:
    """Manages caching of Azure SSO tokens to avoid repeated authentication."""

    def __init__(self):
        self.cache_dir = Path(tempfile.gettempdir()) / "dbt_redshift_tokens"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.chmod(stat.S_IRWXU)
        self.cache_file = self.cache_dir / "azure_token.json"

    def save_token(self, token: str) -> bool:
        """
        Save the IDP token to cache file with expiration time parsed from the token.

        Args:
            token: The JWT token to cache

        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            # Parse token to get expiration time
            token_payload = token.split(".")[1]
            token_payload += "=" * (-len(token_payload) % 4)
            decoded_payload = json.loads(base64.b64decode(token_payload).decode("utf-8"))
            expires_at = datetime.fromtimestamp(decoded_payload.get("exp", 0), tz=timezone.utc)

            cache_data = {"token": token, "expires_at": expires_at.isoformat()}

            with open(self.cache_file, "w") as f:
                json.dump(cache_data, f)

            self.cache_file.chmod(stat.S_IRUSR | stat.S_IWUSR)

            return True
        except (IndexError, ValueError, json.JSONDecodeError, OSError):
            return False

    def load_token(self) -> Optional[str]:
        """
        Load the IDP token from cache file if it exists and is not expired.

        Returns:
            The cached token if valid and not expired, None otherwise
        """
        try:
            if not self.cache_file.exists():
                return None

            with open(self.cache_file, "r") as f:
                cache_data = json.load(f)

            expires_at = datetime.fromisoformat(cache_data["expires_at"]).replace(
                tzinfo=timezone.utc
            )
            if expires_at > datetime.now(timezone.utc):
                return cache_data["token"]
            else:
                self._cleanup_expired_token()
                return None
        except (OSError, json.JSONDecodeError, KeyError, ValueError):
            return None

    def is_token_expired(self, token: str) -> bool:
        """
        Check if a token is expired or about to expire.

        Args:
            token: The JWT token to check

        Returns:
            bool: True if token is expired or will expire within 60 seconds
        """
        try:
            tolerance_seconds = 60
            current_timestamp = int(datetime.now(timezone.utc).timestamp())
            token_payload = token.split(".")[1]
            token_payload += "=" * (-len(token_payload) % 4)
            decoded_payload = json.loads(base64.b64decode(token_payload).decode("utf-8"))
            token_expire_time = decoded_payload.get("exp", 0)

            return token_expire_time - current_timestamp <= tolerance_seconds
        except (IndexError, ValueError, json.JSONDecodeError):
            return True

    def _cleanup_expired_token(self):
        """Remove expired token file."""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
        except OSError:
            pass

    def clear_cache(self):
        """Clear all cached tokens."""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
        except OSError:
            pass


_token_cache = TokenCache()


def save_token_to_cache(token: str) -> bool:
    """Save token to cache. Returns True if successful."""
    return _token_cache.save_token(token)


def load_token_from_cache() -> Optional[str]:
    """Load token from cache. Returns token if valid, None otherwise."""
    return _token_cache.load_token()


def is_token_expired_or_about_to_expire(token: str) -> bool:
    """Check if token is expired or about to expire."""
    return _token_cache.is_token_expired(token)


def clear_token_cache():
    """Clear all cached tokens."""
    _token_cache.clear_cache()
