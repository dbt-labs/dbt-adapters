"""Download and cache the lake compute ADBC driver.

The driver is closed source and is not published to PyPI, so it cannot be a package
dependency; it is fetched from dbt's public CDN instead.

Two things pin it as tightly as a dependency would:

* :data:`DRIVER_VERSION` -- one exact driver version.
* :data:`DRIVER_CHECKSUMS` -- the SHA-256 of every platform artifact for that version. A
  triplet with no entry is refused rather than installed unverified, so a compromised or
  re-cut CDN artifact cannot be loaded.

Resolution is cache-first: a verified driver is written once to the OS cache directory and
every later run just stats it. Nothing here needs credentials -- the CDN is public.
"""

import hashlib
import os
import platform
import stat
import sys
import threading
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("LakeCompute")


#: the pinned driver version; bump it together with DRIVER_CHECKSUMS
DRIVER_VERSION = "0.4.0+dbt0.1.13.g2679dc0"

#: CDN name of the lake compute driver: the artifact is adbc_driver_dbt, not
#: adbc_driver_lakecompute.
DRIVER_NAME = "dbt"

CDN_HOST = "public.cdn.getdbt.com"

#: ``${cache_dir}/com.getdbt/adbc/...``, the cache location shared with other dbt tools, so a
#: machine that already has the driver downloads nothing.
APP_ID = "com.getdbt"

#: points the cache at a pre-populated directory, for CI images and air-gapped installs
CACHE_DIR_ENV_VAR = "DBT_COMPUTE_DRIVER_CACHE"

DOWNLOAD_TIMEOUT_SECONDS = 60

_CHUNK_SIZE = 256 * 1024

MACOS_TARGET_OS = "apple-darwin"
WINDOWS_TARGET_OS = "pc-windows-msvc"

#: Linux artifacts are labelled with the manylinux image they were built in; the lake compute
#: driver is only built against 2_28.
LINUX_TARGET_OS = "manylinux_2_28-linux-gnu"

#: SHA-256 of the **compressed** (.zst) artifact, keyed by (name, os, arch, version).
DRIVER_CHECKSUMS: Dict[Tuple[str, str, str, str], str] = {
    ("dbt", "apple-darwin", "aarch64", DRIVER_VERSION): (
        "9d6a67e4bd81850815ef7cbcc3ce321310e96dfb8ddc12f5c59ee65126fc34a9"
    ),
    ("dbt", "apple-darwin", "x86_64", DRIVER_VERSION): (
        "001efa55df3504d9345009f38ce5127169fa598f7514484c9587ab8ffea8195d"
    ),
    ("dbt", "manylinux_2_28-linux-gnu", "aarch64", DRIVER_VERSION): (
        "c1a5875ebd89fe354809309579f7c9ed92090e065b5dbaa972ee9abc9d74a5b7"
    ),
    ("dbt", "manylinux_2_28-linux-gnu", "x86_64", DRIVER_VERSION): (
        "2fd78d20bf1cfe93ce917ca8814bc4a5d1a56786d0fbdab4979219add27b1763"
    ),
    ("dbt", "pc-windows-msvc", "x86_64", DRIVER_VERSION): (
        "1db3fd52604dc42c6e373af2892a04cc06a9a63457e0e27ffbc59d1defc3642a"
    ),
}


class DriverInstallError(Exception):
    """Anything that stopped a verified driver from reaching the cache."""


@dataclass(frozen=True)
class DriverTriplet:
    """Identifies one CDN artifact."""

    os: str
    arch: str
    version: str

    @property
    def dll_prefix(self) -> str:
        return "" if self.os == WINDOWS_TARGET_OS else "lib"

    @property
    def dll_suffix(self) -> str:
        if self.os == MACOS_TARGET_OS:
            return ".dylib"
        if self.os == WINDOWS_TARGET_OS:
            return ".dll"
        return ".so"

    def filename(self, name: str) -> str:
        return f"{self.dll_prefix}adbc_driver_{name}-{self.version}{self.dll_suffix}"


def _normalized_arch() -> str:
    """``platform.machine()`` in the spelling the CDN uses."""
    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        return "aarch64"
    if machine in ("x86_64", "amd64"):
        return "x86_64"
    raise DriverInstallError(
        f"Unsupported CPU architecture {platform.machine()!r} for the lake compute driver. "
        f"Prebuilt drivers exist for aarch64 and x86_64 only."
    )


def current_triplet() -> DriverTriplet:
    """The artifact to fetch for this machine."""
    arch = _normalized_arch()

    if sys.platform == "darwin":
        target_os = MACOS_TARGET_OS
    elif sys.platform == "win32":
        target_os = WINDOWS_TARGET_OS
    elif sys.platform.startswith("linux"):
        target_os = LINUX_TARGET_OS
    else:
        raise DriverInstallError(
            f"Unsupported platform {sys.platform!r} for the lake compute driver. "
            f"Prebuilt drivers exist for macOS, Linux and Windows."
        )

    return DriverTriplet(os=target_os, arch=arch, version=DRIVER_VERSION)


def expected_checksum(name: str, triplet: DriverTriplet) -> Optional[str]:
    return DRIVER_CHECKSUMS.get((name, triplet.os, triplet.arch, triplet.version))


def driver_url(name: str, triplet: DriverTriplet) -> str:
    # the version contains '+', which has to be escaped; '-', '.' and '_' are left alone
    version = quote(triplet.version, safe="-._")
    return (
        f"https://{CDN_HOST}/fs/adbc/{name}/"
        f"adbc_driver_{name}-{version}-{triplet.arch}-{triplet.os}{triplet.dll_suffix}.zst"
    )


def _os_cache_dir() -> Path:
    """The OS cache directory, or ``$DBT_COMPUTE_DRIVER_CACHE`` when set."""
    override = os.getenv(CACHE_DIR_ENV_VAR)
    if override:
        return Path(override)

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches"
    if sys.platform == "win32":
        local_app_data = os.getenv("LOCALAPPDATA")
        if not local_app_data:
            raise DriverInstallError(
                "Unable to determine the cache directory for the lake compute driver: "
                "%LOCALAPPDATA% is unset."
            )
        return Path(local_app_data)
    return Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache")


def driver_cache_path(name: str, triplet: DriverTriplet, cache_dir: Optional[Path] = None) -> Path:
    base = Path(cache_dir) if cache_dir is not None else _os_cache_dir()
    return base / APP_ID / "adbc" / f"{triplet.arch}-{triplet.os}" / triplet.filename(name)


def _decompressor() -> Any:
    """An incremental zstd decompressor exposing ``.decompress(bytes) -> bytes``."""
    try:
        from compression import zstd  # type: ignore[import-not-found]  # Python 3.14+

        return zstd.ZstdDecompressor()
    except ImportError:
        pass

    try:
        import zstandard
    except ImportError as exc:  # pragma: no cover - zstandard is a hard dependency < 3.14
        raise DriverInstallError(
            "Decompressing the lake compute driver needs the `zstandard` package. "
            "Install it with `pip install zstandard`, or set `driver_path` in your profile "
            "to a driver you already have on disk."
        ) from exc

    return zstandard.ZstdDecompressor().decompressobj()


#: serialises concurrent installs within one process. dbt resolves the driver once per thread
#: connection, so without this every thread would race on the same download.
_install_lock = threading.Lock()


def ensure_driver(
    name: str = DRIVER_NAME,
    triplet: Optional[DriverTriplet] = None,
    cache_dir: Optional[Path] = None,
) -> str:
    """Return a path to the verified driver, downloading it if the cache is cold."""
    triplet = triplet or current_triplet()
    destination = driver_cache_path(name, triplet, cache_dir)

    if destination.exists():
        return str(destination)

    with _install_lock:
        # another thread may have installed it while this one waited
        if destination.exists():
            return str(destination)
        _download(name, triplet, destination)

    return str(destination)


def _download(name: str, triplet: DriverTriplet, destination: Path) -> None:
    checksum = expected_checksum(name, triplet)
    url = driver_url(name, triplet)

    if checksum is None:
        # Installing unverified is worse than failing: it is the one case where a wrong binary
        # gets loaded into the process silently.
        raise DriverInstallError(
            f"No pinned SHA-256 checksum for the lake compute driver "
            f"{triplet.filename(name)} ({triplet.arch}-{triplet.os}), so it will not be "
            f"downloaded. Set `driver_path` in your profile to a driver you have on disk."
        )

    logger.info(f"Downloading the lake compute ADBC driver {triplet.version} from {url}")

    parent = destination.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DriverInstallError(f"Unable to create directory {parent}: {exc}") from exc

    # written alongside the destination so the rename is atomic (same filesystem)
    tmp_path = parent / f".{os.urandom(8).hex()}.download"

    try:
        _stream_to_file(url, tmp_path, checksum)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    _finalize(tmp_path, destination)


def _stream_to_file(url: str, tmp_path: Path, expected_sha256: str) -> None:
    """Download, decompress and hash in one pass, then verify before anything is renamed."""
    hasher = hashlib.sha256()
    decompressor = _decompressor()

    request = urllib.request.Request(url, headers={"User-Agent": "dbt-lakecompute"})
    try:
        with (
            urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response,
            open(tmp_path, "wb") as handle,
        ):
            while True:
                chunk = response.read(_CHUNK_SIZE)
                if not chunk:
                    break
                # the pinned checksum is of the compressed artifact, so hash what arrived
                hasher.update(chunk)
                handle.write(decompressor.decompress(chunk))
            handle.flush()
            os.fsync(handle.fileno())
    except Exception as exc:
        raise DriverInstallError(
            f"Failed to download the lake compute driver from {url}: {exc}"
        ) from exc

    actual = hasher.hexdigest()
    if actual != expected_sha256:
        raise DriverInstallError(
            f"SHA-256 checksum mismatch for the lake compute driver: "
            f"expected {expected_sha256}, got {actual} (URL: {url})"
        )

    # dlopen only needs read access
    os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def _finalize(tmp_path: Path, destination: Path) -> None:
    """Move a fully verified download over the destination.

    If the rename fails but the destination now exists, another process installed the same
    version-qualified driver and we use theirs. That is the common case on Windows, where
    renaming over a DLL another process has already loaded is denied.
    """
    try:
        os.replace(tmp_path, destination)
    except OSError as exc:
        installed_by_other_process = destination.exists()
        tmp_path.unlink(missing_ok=True)
        if not installed_by_other_process:
            raise DriverInstallError(
                f"Unable to install the lake compute driver at {destination}: {exc}"
            ) from exc
