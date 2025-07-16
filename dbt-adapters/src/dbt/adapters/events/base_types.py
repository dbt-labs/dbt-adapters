from dbt_common.events.base_types import BaseEvent
from dbt_common.events.base_types import DebugLevel as CommonDebugLevel
from dbt_common.events.base_types import DynamicLevel as CommonDynamicLevel
from dbt_common.events.base_types import ErrorLevel as CommonErrorLevel
from dbt_common.events.base_types import InfoLevel as CommonInfoLevel
from dbt_common.events.base_types import TestLevel as CommonTestLevel
from dbt_common.events.base_types import WarnLevel as CommonWarnLevel
from dbt.adapters.events import adapter_types_pb2


class AdapterBaseEvent(BaseEvent):
    PROTO_TYPES_MODULE = adapter_types_pb2


class DynamicLevel(CommonDynamicLevel, AdapterBaseEvent):
    pass


class TestLevel(CommonTestLevel, AdapterBaseEvent):
    pass


class DebugLevel(CommonDebugLevel, AdapterBaseEvent):
    pass


class InfoLevel(CommonInfoLevel, AdapterBaseEvent):
    pass


class WarnLevel(CommonWarnLevel, AdapterBaseEvent):
    pass


class ErrorLevel(CommonErrorLevel, AdapterBaseEvent):
    pass
