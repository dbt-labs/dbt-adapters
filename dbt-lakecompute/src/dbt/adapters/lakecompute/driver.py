"""Resolve the ADBC driver shared library and the database options to open it with.

The driver is downloaded from dbt's public CDN on first use (see
:mod:`dbt.adapters.lakecompute.install`), unless ``driver_path`` names one explicitly.
"""

from dataclasses import dataclass
from typing import Dict, TYPE_CHECKING

from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.lakecompute.auth import auth_options
from dbt.adapters.lakecompute.install import DRIVER_VERSION, DriverInstallError, ensure_driver

if TYPE_CHECKING:
    from dbt.adapters.lakecompute.credentials import LakeComputeCredentials

ENTRYPOINT = "AdbcDriverDbtInit"


@dataclass(frozen=True)
class ResolvedDriver:
    """Everything needed to open an ``AdbcDatabase``, and nothing else."""

    path: str
    entrypoint: str
    db_kwargs: Dict[str, str]


def resolve_driver(credentials: "LakeComputeCredentials") -> ResolvedDriver:
    # validate auth before touching the network, so a missing token is reported without a
    # driver download
    db_kwargs: Dict[str, str] = {"uri": credentials.resolved_base_url}
    db_kwargs.update(auth_options(credentials))
    db_kwargs.update(connection_options(credentials))

    if credentials.driver_path:
        path = credentials.driver_path
    else:
        try:
            path = ensure_driver()
        except DriverInstallError as exc:
            raise FailedToConnectError(
                f"Could not obtain the lake compute ADBC driver ({DRIVER_VERSION}): {exc}"
            ) from exc

    return ResolvedDriver(path=path, entrypoint=ENTRYPOINT, db_kwargs=db_kwargs)


def connection_options(credentials: "LakeComputeCredentials") -> Dict[str, str]:
    """Pass-through ``adbc.dbt.*`` database options, omitting anything unset."""
    options: Dict[str, str] = {}
    for field_name, option in (
        ("warehouse", "adbc.dbt.warehouse"),
        ("dialect", "adbc.dbt.dialect"),
        ("timeout_seconds", "adbc.dbt.timeout_seconds"),
    ):
        value = getattr(credentials, field_name, None)
        if value is not None and value != "":
            options[option] = str(value)
    return options
