from typing import TYPE_CHECKING, Tuple

from dbt.adapters.contracts.connection import AdapterResponse

import agate

from mashumaro.types import SerializationStrategy

class AdapterExecuteSerializer(SerializationStrategy):
    def serialize(self, table: Tuple[AdapterResponse, agate.Table]):
        adapter_response, agate_table = table
        return {
            "adapter_response": adapter_response.to_dict(),
            "table": {
                "column_names": agate_table.column_names,
                "column_types": [t.__class__.__name__ for t in agate_table.column_types],
                "rows": list(map(list, agate_table.rows))
            }
        }

    def deserialize(self, data):
        adapter_response_dct, agate_table_dct = data
        return None

