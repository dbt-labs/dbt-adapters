from dbt.adapters.base import AdapterPlugin

from dbt.adapters.lakecompute.connections import (  # noqa: F401
    LakeComputeAdapterResponse,
    LakeComputeConnectionManager,
)
from dbt.adapters.lakecompute.credentials import LakeComputeCredentials  # noqa: F401
from dbt.adapters.lakecompute.relation import LakeComputeRelation  # noqa: F401
from dbt.adapters.lakecompute.impl import LakeComputeAdapter
from dbt.include import lakecompute


Plugin: AdapterPlugin = AdapterPlugin(
    adapter=LakeComputeAdapter,  # type: ignore[arg-type]
    credentials=LakeComputeCredentials,
    include_path=lakecompute.PACKAGE_PATH,
)
