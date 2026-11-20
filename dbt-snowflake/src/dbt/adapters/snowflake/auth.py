import base64
import re
import sys
from typing import Any, Callable, Optional

if sys.version_info < (3, 9):
    from functools import lru_cache

    cache = lru_cache(maxsize=None)
else:
    from functools import cache

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Snowflake")

NON_STANDARD_KEY_MESSAGE = (
    "The Snowflake private key was loaded through a fallback because its encoding is rejected "
    "by the `cryptography` library, for example a legacy PBES1 / PKCS#12 encryption scheme, a "
    "PBES2 key with an explicitly encoded default PRF, or a key encrypted with single DES. "
    "Support for such keys may be removed in a future release. Re-encrypt the key with a "
    "modern scheme, for example: "
    "`openssl pkcs8 -topk8 -v2 aes256 -in old_key.pem -out new_key.pem` "
    "(on OpenSSL 3, also pass `-provider default -provider legacy` if the old key uses "
    "legacy algorithms)"
)

_PEM_BLOCK_PATTERN = re.compile(
    rb"-----BEGIN (?:ENCRYPTED PRIVATE|PRIVATE|RSA PRIVATE|EC PRIVATE) KEY-----.+?"
    rb"-----END (?:ENCRYPTED PRIVATE|PRIVATE|RSA PRIVATE|EC PRIVATE) KEY-----",
    re.DOTALL,
)


def _extract_pem_block(data: bytes) -> bytes:
    """Return just the private-key PEM block, without any surrounding text.

    Returns data unchanged when no PEM block is present.
    """
    match = _PEM_BLOCK_PATTERN.search(data)
    return match.group(0) if match else data


def _der_to_encrypted_pem(der: bytes) -> bytes:
    # pyOpenSSL cannot load encrypted DER, so wrap it in PEM headers
    body = base64.encodebytes(der)
    return (
        b"-----BEGIN ENCRYPTED PRIVATE KEY-----\n"
        + body
        + b"-----END ENCRYPTED PRIVATE KEY-----\n"
    )


def _load_private_key(
    loader: Callable[..., Any],
    data: bytes,
    password: Optional[bytes],
    is_pem: bool,
) -> PrivateKeyTypes:
    try:
        return loader(data=data, password=password, backend=default_backend())
    except (ValueError, UnsupportedAlgorithm) as e:
        # cryptography>=45 parses keys strictly and no longer loads some that
        # older versions and OpenSSL accept, e.g. PBES1 and PKCS#12 PBE
        # (pyca/cryptography#12949), PBES2 with an explicitly encoded default PRF
        # ("DEFAULT value was explicitly encoded"), single-DES traditional PEM.
        # OpenSSL still loads them, so retry through pyOpenSSL.
        original_error = e

    from OpenSSL import crypto  # type: ignore[import-untyped,unused-ignore]

    pem = _extract_pem_block(data) if is_pem else _der_to_encrypted_pem(data)
    try:
        # an empty passphrase stops OpenSSL from prompting on the terminal
        key = crypto.load_privatekey(crypto.FILETYPE_PEM, pem, password or b"")
        fallback_key = key.to_cryptography_key()
    except (crypto.Error, ValueError, TypeError, UnsupportedAlgorithm):
        # keep cryptography's error, e.g. for a wrong passphrase
        raise original_error

    logger.warning(NON_STANDARD_KEY_MESSAGE)
    return fallback_key


@cache
def private_key_from_string(
    private_key_string: str, passphrase: Optional[str] = None
) -> PrivateKeyTypes:

    if passphrase:
        encoded_passphrase = passphrase.encode()
    else:
        encoded_passphrase = None

    if private_key_string.startswith("-"):
        return _load_private_key(
            serialization.load_pem_private_key,
            bytes(private_key_string, "utf-8"),
            encoded_passphrase,
            is_pem=True,
        )
    return _load_private_key(
        serialization.load_der_private_key,
        base64.b64decode(private_key_string),
        encoded_passphrase,
        is_pem=False,
    )


@cache
def private_key_from_file(
    private_key_path: str, passphrase: Optional[str] = None
) -> PrivateKeyTypes:

    if passphrase:
        encoded_passphrase = passphrase.encode()
    else:
        encoded_passphrase = None

    with open(private_key_path, "rb") as file:
        private_key_bytes = file.read()

    return _load_private_key(
        serialization.load_pem_private_key,
        private_key_bytes,
        encoded_passphrase,
        is_pem=True,
    )
