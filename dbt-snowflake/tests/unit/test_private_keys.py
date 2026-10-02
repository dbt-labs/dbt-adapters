import base64
import os
import sys
import tempfile
from typing import Generator
from unittest import mock

from Crypto.Cipher import ARC2, DES
from Crypto.Hash import MD2, MD5, SHA1
from Crypto.IO import PEM
from Crypto.Protocol.KDF import PBKDF1
from Crypto.Util.asn1 import DerObjectId, DerOctetString, DerSequence
from Crypto.Util.Padding import pad
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import pytest

from dbt.adapters.snowflake import auth
from dbt.adapters.snowflake.auth import private_key_from_file, private_key_from_string


PASSPHRASE = "password1234"

# non-ASCII passphrase: must be passed to the fallback byte-for-byte (UTF-8),
# exactly as cryptography receives it, not as a str
NON_ASCII_PASSPHRASE = "pässwörd-密码🔑"


def serialize(private_key: rsa.RSAPrivateKey) -> bytes:
    return private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


@pytest.fixture(scope="session")
def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def private_key_string(private_key) -> str:
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(PASSPHRASE.encode()),
    )
    return private_key_bytes.decode("utf-8")


@pytest.fixture(scope="session")
def private_key_file(private_key) -> Generator[str, None, None]:
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(PASSPHRASE.encode()),
    )
    file = tempfile.NamedTemporaryFile()
    file.write(private_key_bytes)
    file.seek(0)
    yield file.name
    file.close()


def test_private_key_from_string_pem(private_key_string, private_key):
    assert isinstance(private_key_string, str)
    calculated_private_key = private_key_from_string(private_key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


@pytest.mark.skipif(sys.platform == "win32", reason="permission issues on Windows")
def test_private_key_from_file(private_key_file, private_key):
    assert os.path.exists(private_key_file)
    calculated_private_key = private_key_from_file(private_key_file, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


# Legacy PBES1 (PKCS#5 v1.5) keys, which cryptography>=45 can no longer decrypt.
# cryptography can only serialize keys with PBES2 (BestAvailableEncryption), so
# PBES1-encrypted fixtures are assembled by hand.
PBES1_SCHEMES = {
    "pbeWithMD5AndDES-CBC": ("1.2.840.113549.1.5.3", MD5, DES),
    "pbeWithMD5AndRC2-CBC": ("1.2.840.113549.1.5.6", MD5, ARC2),
    "pbeWithSHA1AndDES-CBC": ("1.2.840.113549.1.5.10", SHA1, DES),
    "pbeWithSHA1AndRC2-CBC": ("1.2.840.113549.1.5.11", SHA1, ARC2),
}

# MD2-based PBES1 schemes: neither cryptography (any version) nor pycryptodome
# can decrypt them, so they are not declared supported and must never take the
# fallback path.
UNSUPPORTED_PBES1_SCHEMES = {
    "pbeWithMD2AndDES-CBC": ("1.2.840.113549.1.5.1", MD2, DES),
}

ALL_PBES1_SCHEMES = {**PBES1_SCHEMES, **UNSUPPORTED_PBES1_SCHEMES}


def pbes1_encrypt(private_key: rsa.RSAPrivateKey, scheme: str, passphrase: str) -> bytes:
    """Return a DER EncryptedPrivateKeyInfo encrypted with the given PBES1 scheme."""
    oid, hash_module, cipher_module = ALL_PBES1_SCHEMES[scheme]
    salt, iterations = os.urandom(8), 2048
    derived = PBKDF1(passphrase.encode(), salt, 16, iterations, hash_module)
    cipher_kwargs = {"effective_keylen": 64} if cipher_module is ARC2 else {}
    cipher = cipher_module.new(derived[:8], cipher_module.MODE_CBC, derived[8:], **cipher_kwargs)
    encrypted = cipher.encrypt(pad(serialize(private_key), 8))
    pbe_params = DerSequence([DerOctetString(salt).encode(), iterations]).encode()
    algorithm = DerSequence([DerObjectId(oid).encode(), pbe_params]).encode()
    return DerSequence([algorithm, DerOctetString(encrypted).encode()]).encode()


def to_pem(der: bytes) -> str:
    return PEM.encode(der, "ENCRYPTED PRIVATE KEY")


@pytest.fixture(autouse=True)
def clear_key_cache():
    private_key_from_string.cache_clear()
    private_key_from_file.cache_clear()


@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_is_pbes1_encrypted(private_key, scheme):
    der = pbes1_encrypt(private_key, scheme, PASSPHRASE)
    assert auth._is_pbes1_encrypted(to_pem(der).encode(), is_pem=True)
    assert auth._is_pbes1_encrypted(der, is_pem=False)


def test_is_pbes1_encrypted_false_for_other_data(private_key, private_key_string):
    unencrypted_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    unencrypted_der = private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    traditional_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    # modern PBES2-encrypted key
    assert not auth._is_pbes1_encrypted(private_key_string.encode(), is_pem=True)
    # unencrypted PKCS#8, PEM and DER forms
    assert not auth._is_pbes1_encrypted(unencrypted_pem, is_pem=True)
    assert not auth._is_pbes1_encrypted(unencrypted_der, is_pem=False)
    # unencrypted PKCS#1 traditional PEM
    assert not auth._is_pbes1_encrypted(traditional_pem, is_pem=True)
    # not a key at all
    assert not auth._is_pbes1_encrypted(b"-----BEGIN PRIVATE KEY-----", is_pem=True)
    assert not auth._is_pbes1_encrypted(b"not a key", is_pem=False)


def test_pbes1_oids_match_supported_schemes():
    # every OID declared as fallback-supported must be exercised by the
    # round-trip tests above, and vice versa
    assert auth._PBES1_OIDS == {oid for oid, _, _ in PBES1_SCHEMES.values()}


@pytest.mark.parametrize("scheme", UNSUPPORTED_PBES1_SCHEMES)
def test_unsupported_pbes1_scheme_does_not_use_fallback(private_key, scheme):
    # MD2-based PBES1 keys cannot be decrypted by the fallback (or by any
    # cryptography version), so they must not be detected as eligible and
    # must fail with cryptography's original error
    key_string = to_pem(pbes1_encrypt(private_key, scheme, PASSPHRASE))
    unsupported = ValueError(f"Unknown key encryption algorithm: {ALL_PBES1_SCHEMES[scheme][0]}")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported),
        mock.patch("Crypto.PublicKey.RSA.import_key") as import_key,
    ):
        with pytest.raises(ValueError) as excinfo:
            private_key_from_string(key_string, PASSPHRASE)
    assert excinfo.value is unsupported
    assert not auth._is_pbes1_encrypted(key_string.encode(), is_pem=True)
    import_key.assert_not_called()


@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_private_key_from_string_pem(private_key, scheme):
    key_string = to_pem(pbes1_encrypt(private_key, scheme, PASSPHRASE))
    calculated_private_key = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_private_key_from_string_der(private_key, scheme):
    key_string = base64.b64encode(pbes1_encrypt(private_key, scheme, PASSPHRASE)).decode()
    calculated_private_key = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


@pytest.mark.skipif(sys.platform == "win32", reason="permission issues on Windows")
@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_private_key_from_file(private_key, scheme, tmp_path):
    key_file = tmp_path / "key.pem"
    key_file.write_text(to_pem(pbes1_encrypt(private_key, scheme, PASSPHRASE)))
    calculated_private_key = private_key_from_file(str(key_file), PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_private_key_wrong_passphrase(private_key, scheme):
    key_string = to_pem(pbes1_encrypt(private_key, scheme, PASSPHRASE))
    with pytest.raises(ValueError):
        private_key_from_string(key_string, "wrong-passphrase")


@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_non_ascii_passphrase(private_key, scheme):
    key_string = to_pem(pbes1_encrypt(private_key, scheme, NON_ASCII_PASSPHRASE))
    calculated_private_key = private_key_from_string(key_string, NON_ASCII_PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


def test_pbes1_non_ascii_passphrase_uses_fallback(private_key):
    # force the fallback regardless of the installed cryptography version
    key_string = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", NON_ASCII_PASSPHRASE))
    unsupported = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported),
        mock.patch.object(auth.logger, "warning") as warning,
    ):
        calculated_private_key = private_key_from_string(key_string, NON_ASCII_PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
    warning.assert_called_once_with(auth.PBES1_DEPRECATION_MESSAGE)


def test_unencrypted_private_key_from_string(private_key):
    key_string = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    calculated_private_key = private_key_from_string(key_string)
    assert serialize(calculated_private_key) == serialize(private_key)


def test_modern_key_does_not_use_fallback(private_key_string, private_key):
    with (
        mock.patch("Crypto.PublicKey.RSA.import_key") as import_key,
        mock.patch.object(auth.logger, "warning") as warning,
    ):
        calculated_private_key = private_key_from_string(private_key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
    import_key.assert_not_called()
    warning.assert_not_called()


def test_unsupported_algorithm_uses_fallback_and_warns(private_key):
    # simulate cryptography>=45 regardless of the installed version
    key_string = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", PASSPHRASE))
    unsupported = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported),
        mock.patch.object(auth.logger, "warning") as warning,
    ):
        calculated_private_key = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
    warning.assert_called_once_with(auth.PBES1_DEPRECATION_MESSAGE)


def test_fallback_triggers_regardless_of_error_wording(private_key):
    # the fallback is selected by parsing the key, so it still works if
    # cryptography rewords its "unknown key encryption algorithm" error
    key_string = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", PASSPHRASE))
    reworded = ValueError("Unsupported encryption scheme for key: 1.2.840.113549.1.5.10")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=reworded),
        mock.patch.object(auth.logger, "warning") as warning,
    ):
        calculated_private_key = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
    warning.assert_called_once_with(auth.PBES1_DEPRECATION_MESSAGE)


def test_error_message_match_alone_does_not_trigger_fallback(private_key_string):
    # a modern (PBES2) key must not take the fallback path, even when the
    # error message mentions an unsupported PBES1-style algorithm
    sneaky = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=sneaky),
        mock.patch("Crypto.PublicKey.RSA.import_key") as import_key,
    ):
        with pytest.raises(ValueError) as excinfo:
            private_key_from_string(private_key_string, PASSPHRASE)
    assert excinfo.value is sneaky
    import_key.assert_not_called()


def test_fallback_failure_raises_original_error(private_key):
    key_string = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", PASSPHRASE))
    unsupported = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported):
        with pytest.raises(ValueError) as excinfo:
            private_key_from_string(key_string, "wrong-passphrase")
    assert excinfo.value is unsupported


def test_other_errors_are_not_caught(private_key):
    other_error = ValueError("Could not deserialize key data.")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=other_error),
        mock.patch("Crypto.PublicKey.RSA.import_key") as import_key,
    ):
        with pytest.raises(ValueError) as excinfo:
            private_key_from_string("-----BEGIN PRIVATE KEY-----", PASSPHRASE)
    assert excinfo.value is other_error
    import_key.assert_not_called()
