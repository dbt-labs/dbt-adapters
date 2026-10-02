import base64
import sys
from typing import Any, Callable, Optional, cast

if sys.version_info < (3, 9):
    from functools import lru_cache

    cache = lru_cache(maxsize=None)
else:
    from functools import cache

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Snowflake")

PBES1_DEPRECATION_MESSAGE = (
    "The Snowflake private key is encrypted with a legacy PBES1 (PKCS#5 v1.5) scheme, "
    "which is insecure and no longer supported by the `cryptography` library. "
    "Support for it will be removed in a future release. Re-encrypt the key with a modern "
    "scheme, for example: `openssl pkcs8 -topk8 -v2 aes256 -in old_key.pem -out new_key.pem` "
    "(on OpenSSL 3, also pass `-provider default -provider legacy`, because PBKDF1 and DES "
    "are only available in the legacy provider)"
)

# PKCS#5 v1.5 encryption schemes (PBES1) from RFC 8018, Appendix A.3, limited
# to the four schemes the pycryptodome fallback can decrypt. cryptography>=45
# can no longer decrypt any of them (pyca/cryptography#12949). The MD2-based
# PBES1 schemes are excluded on purpose: no cryptography version ever decrypted
# them, and pycryptodome cannot either, so such keys fail with cryptography's
# original error regardless of this set.
_PBES1_OIDS = frozenset(
    {
        "1.2.840.113549.1.5.3",  # pbeWithMD5AndDES-CBC
        "1.2.840.113549.1.5.6",  # pbeWithMD5AndRC2-CBC
        "1.2.840.113549.1.5.10",  # pbeWithSHA1AndDES-CBC
        "1.2.840.113549.1.5.11",  # pbeWithSHA1AndRC2-CBC
    }
)


def _is_pbes1_encrypted(data: bytes, is_pem: bool) -> bool:
    """Return True when data is an EncryptedPrivateKeyInfo encrypted with one
    of the PBES1 schemes the pycryptodome fallback can decrypt.

    The scheme is detected by parsing the encryption algorithm OID out of the
    key itself, rather than by matching cryptography's error message, so this
    keeps working even if cryptography rewords how it reports the failure.
    """
    from Crypto.IO import PEM
    from Crypto.Util.asn1 import DerObjectId, DerSequence

    try:
        der = PEM.decode(data.decode())[0] if is_pem else data
        encrypted_key_info = DerSequence().decode(der)
        encryption_algorithm = cast(bytes, encrypted_key_info[0])
        algorithm_id = DerSequence().decode(encryption_algorithm)
        oid = DerObjectId().decode(cast(bytes, algorithm_id[0])).value
    except (ValueError, TypeError, IndexError):
        return False
    return oid in _PBES1_OIDS


def _load_private_key(
    loader: Callable[..., Any],
    data: bytes,
    password: Optional[bytes],
    is_pem: bool,
) -> RSAPrivateKey:
    try:
        return loader(data=data, password=password, backend=default_backend())
    except ValueError as e:
        # cryptography>=45 dropped PBES1 decryption (pyca/cryptography#12949)
        if not _is_pbes1_encrypted(data, is_pem):
            raise
        original_error = e

    from Crypto.PublicKey import RSA

    try:
        # pycryptodome uses a bytes passphrase as-is at runtime; its annotation
        # only declares str, which raises UnicodeEncodeError for non-ASCII
        # passphrases, so pass through the exact bytes cryptography received
        legacy_key = RSA.import_key(data, passphrase=password)  # type: ignore[arg-type]
    except (ValueError, IndexError, TypeError):
        # keep cryptography's error, e.g. for a wrong passphrase
        raise original_error

    logger.warning(PBES1_DEPRECATION_MESSAGE)
    return cast(
        RSAPrivateKey,
        serialization.load_der_private_key(
            data=legacy_key.export_key(format="DER", pkcs=8),
            password=None,
            backend=default_backend(),
        ),
    )


@cache
def private_key_from_string(
    private_key_string: str, passphrase: Optional[str] = None
) -> RSAPrivateKey:

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
) -> RSAPrivateKey:

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
