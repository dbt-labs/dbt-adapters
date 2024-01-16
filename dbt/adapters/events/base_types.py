# Aliasing common Level classes in order to make custom, but not overly-verbose versions that have PROTO_TYPES_MODULE set to the adapter-specific generated types_pb2 module
from dbt_common.events.base_types import (
    BaseEvent,
    DebugLevel as CommonDebugLevel,
    DynamicLevel as CommonDynamicLevel,
    ErrorLevel as CommonErrorLevel,
    InfoLevel as CommonInfoLevel,
    TestLevel as CommonTestLevel,
    WarnLevel as CommonWarnLevel,
)

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
