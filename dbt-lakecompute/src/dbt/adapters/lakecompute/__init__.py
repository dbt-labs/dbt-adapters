from typing import List, Optional

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.factory import AdapterContainer

from dbt.adapters.lakecompute.connections import (  # noqa: F401
    LakeComputeAdapterResponse,
    LakeComputeConnectionManager,
)
from dbt.adapters.lakecompute.credentials import LakeComputeCredentials  # noqa: F401
from dbt.adapters.lakecompute.relation import LakeComputeRelation  # noqa: F401
from dbt.adapters.lakecompute.impl import LakeComputeAdapter
from dbt.include import lakecompute


# Dispatch lakecompute__ -> duckdb__ -> default__, as in v2.
_get_adapter_type_names = AdapterContainer.get_adapter_type_names


def _get_adapter_type_names_with_duckdb(self: AdapterContainer, name: Optional[str]) -> List[str]:
    names = _get_adapter_type_names(self, name)
    if name == "lakecompute":
        names.append("duckdb")
    return names


AdapterContainer.get_adapter_type_names = _get_adapter_type_names_with_duckdb  # type: ignore[method-assign]


Plugin: AdapterPlugin = AdapterPlugin(
    adapter=LakeComputeAdapter,  # type: ignore[arg-type]
    credentials=LakeComputeCredentials,
    include_path=lakecompute.PACKAGE_PATH,
)
