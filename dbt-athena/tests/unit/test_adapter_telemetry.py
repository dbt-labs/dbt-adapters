from unittest import mock

import dbt.adapters.athena.__version__

from dbt.adapters.athena.impl import AthenaAdapter
from dbt.adapters.base.relation import AdapterTrackingRelationInfo


def test_telemetry_with_athena_details():
    mock_model_config = mock.MagicMock()
    mock_model_config._extra = mock.MagicMock()
    mock_model_config._extra = {
        "adapter_type": "athena",
        "table_type": "iceberg",
    }

    res = AthenaAdapter.get_adapter_run_info(mock_model_config)

    assert res.adapter_name == "athena"
    assert res.base_adapter_version == dbt.adapters.__about__.version
    assert res.adapter_version == dbt.adapters.athena.__version__.version

    assert res.model_adapter_details == {
        "adapter_type": "athena",
        "table_format": "iceberg",
    }

    assert isinstance(res, AdapterTrackingRelationInfo)
