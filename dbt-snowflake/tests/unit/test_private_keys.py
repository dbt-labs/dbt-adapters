import base64
import os
import sys
import tempfile
import textwrap
from typing import Generator
from unittest import mock

from Crypto.Cipher import ARC2, DES
from Crypto.Hash import MD5, SHA1
from Crypto.IO import PEM
from Crypto.Protocol.KDF import PBKDF1
from Crypto.Util.asn1 import DerObjectId, DerOctetString, DerSequence
from Crypto.Util.Padding import pad
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
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


def pbes1_encrypt(private_key: rsa.RSAPrivateKey, scheme: str, passphrase: str) -> bytes:
    """Return a DER EncryptedPrivateKeyInfo encrypted with the given PBES1 scheme."""
    oid, hash_module, cipher_module = PBES1_SCHEMES[scheme]
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
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_trailing_text_from_string(private_key, scheme):
    # cryptography ignores text after the PEM block, so such keys load today;
    # once cryptography>=45 drops PBES1, the fallback must load them too.
    # (Text before the block is different: it never reaches the PEM path,
    # because private_key_from_string only routes strings that start with
    # "-" to the PEM loader.)
    clean = to_pem(pbes1_encrypt(private_key, scheme, PASSPHRASE))
    key_string = f"{clean}\ntrailing text"
    calculated_private_key = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


def test_pbes1_trailing_text_uses_fallback(private_key):
    # force the fallback regardless of the installed cryptography version
    clean = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", PASSPHRASE))
    key_string = f"{clean}\ntrailing text"
    unsupported = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported),
        mock.patch.object(auth.logger, "warning") as warning,
    ):
        calculated_private_key = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


@pytest.mark.skipif(sys.platform == "win32", reason="permission issues on Windows")
@pytest.mark.parametrize("scheme", PBES1_SCHEMES)
def test_pbes1_surrounding_text_from_file(private_key, scheme, tmp_path):
    # files go straight to the PEM loader, which scans for the PEM block
    # inside surrounding text, so the fallback must strip that text too
    clean = to_pem(pbes1_encrypt(private_key, scheme, PASSPHRASE))
    key_file = tmp_path / "key.pem"
    key_file.write_text(f"leading text\n{clean}\ntrailing text")
    calculated_private_key = private_key_from_file(str(key_file), PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


def test_unencrypted_private_key_from_string(private_key):
    key_string = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    calculated_private_key = private_key_from_string(key_string)
    assert serialize(calculated_private_key) == serialize(private_key)


def test_modern_key_does_not_use_fallback(private_key_string, private_key):
    with mock.patch.object(auth.logger, "warning") as warning:
        calculated_private_key = private_key_from_string(private_key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
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
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


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
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


def test_modern_key_rejected_by_cryptography_uses_generic_warning(private_key_string, private_key):
    # a PBES2 key is not reported as PBES1, whatever the error message says
    sneaky = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with (
        mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=sneaky),
        mock.patch.object(auth.logger, "warning") as warning,
    ):
        loaded = private_key_from_string(private_key_string, PASSPHRASE)
    assert serialize(loaded) == serialize(private_key)
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


def test_fallback_failure_raises_original_error(private_key):
    key_string = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", PASSPHRASE))
    unsupported = ValueError("Unknown key encryption algorithm: 1.2.840.113549.1.5.10")
    with mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported):
        with pytest.raises(ValueError) as excinfo:
            private_key_from_string(key_string, "wrong-passphrase")
    assert excinfo.value is unsupported


def test_unparseable_key_raises_original_error():
    other_error = ValueError("Could not deserialize key data.")
    with mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=other_error):
        with pytest.raises(ValueError) as excinfo:
            private_key_from_string("-----BEGIN PRIVATE KEY-----", PASSPHRASE)
    assert excinfo.value is other_error


# PBES2 key whose PBKDF2 parameters explicitly encode the default PRF
# (hmacWithSHA1). OpenSSL accepts it; cryptography>=45 rejects it with
# "DEFAULT value was explicitly encoded", before the passphrase is used.
def pbes2_explicit_prf_encrypt(private_key, passphrase: str) -> bytes:
    from Crypto.Cipher import AES
    from Crypto.Protocol.KDF import PBKDF2

    salt, iv, iterations = os.urandom(8), os.urandom(16), 2048
    derived = PBKDF2(passphrase.encode(), salt, 32, iterations)  # default HMAC-SHA1
    encrypted = AES.new(derived, AES.MODE_CBC, iv).encrypt(pad(serialize(private_key), 16))
    prf = DerSequence([DerObjectId("1.2.840.113549.2.7").encode(), b"\x05\x00"]).encode()
    pbkdf2_params = DerSequence([DerOctetString(salt).encode(), iterations, prf]).encode()
    kdf = DerSequence([DerObjectId("1.2.840.113549.1.5.12").encode(), pbkdf2_params]).encode()
    cipher = DerSequence(
        [DerObjectId("2.16.840.1.101.3.4.1.42").encode(), DerOctetString(iv).encode()]
    ).encode()
    pbes2_params = DerSequence([kdf, cipher]).encode()
    algorithm = DerSequence([DerObjectId("1.2.840.113549.1.5.13").encode(), pbes2_params]).encode()
    return DerSequence([algorithm, DerOctetString(encrypted).encode()]).encode()


@pytest.fixture(scope="session")
def ec_private_key():
    return ec.generate_private_key(ec.SECP256R1())


@pytest.mark.parametrize("key_fixture", ["private_key", "ec_private_key"])
@pytest.mark.parametrize("is_pem", [True, False])
def test_pbes2_explicit_prf(key_fixture, is_pem, request):
    key = request.getfixturevalue(key_fixture)
    der = pbes2_explicit_prf_encrypt(key, PASSPHRASE)
    with mock.patch.object(auth.logger, "warning") as warning:
        if is_pem:
            loaded = private_key_from_string(to_pem(der), PASSPHRASE)
        else:
            loaded = private_key_from_string(base64.b64encode(der).decode(), PASSPHRASE)
    assert serialize(loaded) == serialize(key)
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


@pytest.mark.skipif(sys.platform == "win32", reason="permission issues on Windows")
def test_pbes2_explicit_prf_from_file(ec_private_key, tmp_path):
    path = tmp_path / "key.pem"
    path.write_text(to_pem(pbes2_explicit_prf_encrypt(ec_private_key, PASSPHRASE)))
    assert serialize(private_key_from_file(str(path), PASSPHRASE)) == serialize(ec_private_key)


def test_pbes2_explicit_prf_wrong_passphrase_raises(private_key):
    der = pbes2_explicit_prf_encrypt(private_key, PASSPHRASE)
    with pytest.raises(ValueError):
        private_key_from_string(to_pem(der), "wrong-passphrase")


def single_des_traditional_pem(private_key, passphrase: str) -> str:
    # pycryptodome's PEM.encode only writes DES-EDE3-CBC, so build the
    # `DEK-Info: DES-CBC` PEM by hand: OpenSSL EVP_BytesToKey (one MD5 round,
    # first 8 IV bytes as salt) derives the key
    der = private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    iv = os.urandom(8)
    key = MD5.new(passphrase.encode() + iv).digest()[:8]
    encrypted = DES.new(key, DES.MODE_CBC, iv).encrypt(pad(der, DES.block_size))
    body = base64.b64encode(encrypted).decode()
    lines = textwrap.wrap(body, 64)
    return (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "Proc-Type: 4,ENCRYPTED\n"
        f"DEK-Info: DES-CBC,{iv.hex().upper()}\n\n"
        + "\n".join(lines)
        + "\n-----END RSA PRIVATE KEY-----\n"
    )


def test_single_des_traditional_pem(private_key):
    # cryptography>=45 rejects traditional PEM encrypted with single DES
    # ("Key encrypted with unknown cipher")
    pem = single_des_traditional_pem(private_key, PASSPHRASE)
    assert "DEK-Info: DES-CBC," in pem
    with pytest.raises((ValueError, UnsupportedAlgorithm)):
        serialization.load_pem_private_key(pem.encode(), PASSPHRASE.encode())
    with mock.patch.object(auth.logger, "warning") as warning:
        loaded = private_key_from_string(pem, PASSPHRASE)
    assert serialize(loaded) == serialize(private_key)
    warning.assert_called_once_with(auth.NON_STANDARD_KEY_MESSAGE)


# EC P-256 key encrypted with the PKCS#12 PBE pbeWithSHA1And2-KeyTripleDES-CBC
# (`openssl pkcs8 -topk8 -v1 PBE-SHA1-2DES`), which cryptography>=45 cannot load.
PKCS12_2DES_KEY = """-----BEGIN ENCRYPTED PRIVATE KEY-----
MIGxMBwGCiqGSIb3DQEMAQQwDgQI67mLpjBKN1MCAggABIGQQcWuCcT26ANhKdaj
26VQU8sm8i6npue+wzG95L8z4DNTSKD4x6cOIcJtAethMmY3WXOFiAq+hJH1uo0i
ZkwQEaK8TjqcgfA1ta8u251zkhS0IlqZgQxBEmpZZCOUh2U7126lCJZBjyd5zdZD
+aehZn79a1yyd1cYOm0MFujo0uybLYUDoVipdl02yay3Ygjh
-----END ENCRYPTED PRIVATE KEY-----
"""
PKCS12_2DES_PUBLIC_KEY = b"""-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGLYUWAHMGvso2g8TMYH8oJ0pcCpp
rnI73QFk+DNCFiy3JDRf/7FvD717VAIiPHJSuQNz/BjW3UT9jMetxaf4Vw==
-----END PUBLIC KEY-----
"""


def test_pkcs12_pbe_2key_3des():
    loaded = private_key_from_string(PKCS12_2DES_KEY, PASSPHRASE)
    assert (
        loaded.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        == PKCS12_2DES_PUBLIC_KEY
    )


def test_pkcs12_pbe_2key_3des_wrong_passphrase_raises():
    with pytest.raises(ValueError):
        private_key_from_string(PKCS12_2DES_KEY, "wrong-passphrase")


def test_explicit_prf_without_passphrase_raises_without_prompting(private_key):
    # the strict ASN.1 error comes before cryptography checks the passphrase, so
    # the fallback must not let OpenSSL prompt for one on the terminal
    der = pbes2_explicit_prf_encrypt(private_key, PASSPHRASE)
    with pytest.raises(ValueError):
        private_key_from_string(to_pem(der))


def test_unsupported_algorithm_error_uses_fallback(private_key):
    unsupported = UnsupportedAlgorithm("explicit curve encoding")
    key_string = to_pem(pbes1_encrypt(private_key, "pbeWithSHA1AndDES-CBC", PASSPHRASE))
    with mock.patch.object(auth.serialization, "load_pem_private_key", side_effect=unsupported):
        loaded = private_key_from_string(key_string, PASSPHRASE)
    assert serialize(loaded) == serialize(private_key)
