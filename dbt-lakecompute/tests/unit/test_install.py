import hashlib
import io
import os
import stat
from pathlib import Path

import pytest

from dbt.adapters.lakecompute import install
from dbt.adapters.lakecompute.install import (
    DRIVER_CHECKSUMS,
    DRIVER_NAME,
    DRIVER_VERSION,
    DriverInstallError,
    DriverTriplet,
    current_triplet,
    driver_cache_path,
    driver_url,
    ensure_driver,
    expected_checksum,
)

DARWIN = DriverTriplet(os="apple-darwin", arch="aarch64", version=DRIVER_VERSION)
LINUX = DriverTriplet(os="manylinux_2_28-linux-gnu", arch="x86_64", version=DRIVER_VERSION)
WINDOWS = DriverTriplet(os="pc-windows-msvc", arch="x86_64", version=DRIVER_VERSION)


def zstd_compress(payload: bytes) -> bytes:
    try:
        from compression import zstd  # Python 3.14+

        return zstd.compress(payload)
    except ImportError:
        pass
    zstandard = pytest.importorskip("zstandard")
    return zstandard.ZstdCompressor().compress(payload)


class FakeResponse(io.BytesIO):
    """Just enough of an http.client.HTTPResponse for urlopen's context manager."""

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@pytest.fixture
def served(monkeypatch):
    """Serve bytes from a fake CDN and record the URL that was requested."""
    requested = []

    def serve(payload: bytes):
        def fake_urlopen(request, timeout=None):
            requested.append(request.full_url)
            return FakeResponse(payload)

        monkeypatch.setattr(install.urllib.request, "urlopen", fake_urlopen)
        return requested

    return serve


class TestTriplet:
    def test_library_naming_per_platform(self):
        assert DARWIN.filename("dbt") == f"libadbc_driver_dbt-{DRIVER_VERSION}.dylib"
        assert LINUX.filename("dbt") == f"libadbc_driver_dbt-{DRIVER_VERSION}.so"
        # no `lib` prefix on Windows
        assert WINDOWS.filename("dbt") == f"adbc_driver_dbt-{DRIVER_VERSION}.dll"

    def test_every_pinned_checksum_is_64_hex_characters(self):
        for key, checksum in DRIVER_CHECKSUMS.items():
            assert len(checksum) == 64, key
            assert set(checksum) <= set("0123456789abcdef"), key

    def test_this_platform_resolves_to_a_pinned_artifact(self):
        # a triplet with no checksum would be refused at download time, so the table has to
        # cover whatever platform the tests run on
        assert expected_checksum(DRIVER_NAME, current_triplet()) is not None

    def test_unknown_architecture_is_refused(self, monkeypatch):
        monkeypatch.setattr(install.platform, "machine", lambda: "sparc64")
        with pytest.raises(DriverInstallError, match="Unsupported CPU architecture"):
            current_triplet()

    def test_unknown_platform_is_refused(self, monkeypatch):
        monkeypatch.setattr(install.sys, "platform", "aix")
        with pytest.raises(DriverInstallError, match="Unsupported platform"):
            current_triplet()

    def test_linux_uses_the_manylinux_2_28_build(self, monkeypatch):
        monkeypatch.setattr(install.sys, "platform", "linux")
        monkeypatch.setattr(install.platform, "machine", lambda: "x86_64")
        assert current_triplet().os == "manylinux_2_28-linux-gnu"


class TestUrl:
    def test_version_plus_is_percent_encoded(self):
        url = driver_url("dbt", DARWIN)
        assert "+" not in url
        assert "0.4.0%2Bdbt0.1.13.g2679dc0" in url

    def test_url_template(self):
        assert driver_url("dbt", LINUX) == (
            "https://public.cdn.getdbt.com/fs/adbc/dbt/"
            "adbc_driver_dbt-0.4.0%2Bdbt0.1.13.g2679dc0-x86_64-manylinux_2_28-linux-gnu.so.zst"
        )


class TestCachePath:
    def test_layout(self, tmp_path):
        path = driver_cache_path("dbt", DARWIN, cache_dir=tmp_path)
        assert path == (
            tmp_path
            / "com.getdbt"
            / "adbc"
            / "aarch64-apple-darwin"
            / f"libadbc_driver_dbt-{DRIVER_VERSION}.dylib"
        )

    def test_env_var_overrides_the_os_cache_dir(self, monkeypatch, tmp_path):
        monkeypatch.setenv(install.CACHE_DIR_ENV_VAR, str(tmp_path))
        assert driver_cache_path("dbt", DARWIN).is_relative_to(tmp_path)


class TestEnsureDriver:
    def test_downloads_decompresses_and_verifies(self, monkeypatch, served, tmp_path):
        # larger than the 256 KiB read size, so the streaming path is actually exercised
        payload = b"ELF-ish driver bytes" * 50_000
        compressed = zstd_compress(payload)
        triplet = DriverTriplet(os="apple-darwin", arch="aarch64", version="test")
        monkeypatch.setitem(
            DRIVER_CHECKSUMS,
            ("dbt", triplet.os, triplet.arch, triplet.version),
            hashlib.sha256(compressed).hexdigest(),
        )

        requested = served(compressed)
        path = ensure_driver("dbt", triplet, cache_dir=tmp_path)

        assert Path(path).read_bytes() == payload
        assert requested == [driver_url("dbt", triplet)]
        # nothing but the driver itself is left in the cache directory
        assert [p.name for p in Path(path).parent.iterdir()] == [triplet.filename("dbt")]

    def test_a_cached_driver_is_not_re_downloaded(self, monkeypatch, tmp_path):
        def no_network(*args, **kwargs):
            raise AssertionError("a cache hit must not touch the network")

        monkeypatch.setattr(install.urllib.request, "urlopen", no_network)

        destination = driver_cache_path("dbt", DARWIN, cache_dir=tmp_path)
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"already here")

        assert ensure_driver("dbt", DARWIN, cache_dir=tmp_path) == str(destination)

    def test_checksum_mismatch_leaves_nothing_behind(self, served, tmp_path):
        served(zstd_compress(b"not the pinned artifact"))

        with pytest.raises(DriverInstallError, match="checksum mismatch"):
            ensure_driver("dbt", DARWIN, cache_dir=tmp_path)

        # a driver that failed verification must never be loadable
        assert not driver_cache_path("dbt", DARWIN, cache_dir=tmp_path).exists()
        leftovers = list((tmp_path / "com.getdbt" / "adbc").rglob("*"))
        assert [p for p in leftovers if p.is_file()] == []

    def test_a_triplet_with_no_pinned_checksum_is_refused(self, monkeypatch, tmp_path):
        def no_network(*args, **kwargs):
            raise AssertionError("an unpinned artifact must not be downloaded at all")

        monkeypatch.setattr(install.urllib.request, "urlopen", no_network)
        unpinned = DriverTriplet(os="apple-darwin", arch="aarch64", version="99.0.0")

        with pytest.raises(DriverInstallError, match="No pinned SHA-256 checksum"):
            ensure_driver("dbt", unpinned, cache_dir=tmp_path)

    def test_a_download_failure_leaves_nothing_behind(self, monkeypatch, tmp_path):
        def boom(*args, **kwargs):
            raise OSError("connection reset")

        monkeypatch.setattr(install.urllib.request, "urlopen", boom)

        with pytest.raises(DriverInstallError, match="Failed to download"):
            ensure_driver("dbt", DARWIN, cache_dir=tmp_path)

        parent = driver_cache_path("dbt", DARWIN, cache_dir=tmp_path).parent
        assert list(parent.iterdir()) == []

    @pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
    def test_the_installed_driver_is_readable(self, monkeypatch, served, tmp_path):
        compressed = zstd_compress(b"driver")
        triplet = DriverTriplet(os="apple-darwin", arch="aarch64", version="test")
        monkeypatch.setitem(
            DRIVER_CHECKSUMS,
            ("dbt", triplet.os, triplet.arch, triplet.version),
            hashlib.sha256(compressed).hexdigest(),
        )
        served(compressed)

        path = ensure_driver("dbt", triplet, cache_dir=tmp_path)
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o644
