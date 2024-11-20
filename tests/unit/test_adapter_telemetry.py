import dbt.adapters.__about__

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.relation import AdapterTrackingRelationInfo


def test_telemetry_returns():
    res = BaseAdapter.get_adapter_run_info({})

    assert res.adapter_name == "base"
    assert res.base_adapter_version == dbt.adapters.__about__.version
    assert res.adapter_version == ""
    assert res.model_adapter_details == {}

    assert type(res) is AdapterTrackingRelationInfo
