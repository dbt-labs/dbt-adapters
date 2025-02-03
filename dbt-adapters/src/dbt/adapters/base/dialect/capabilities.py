from dbt.adapters.capability import CapabilityDict


class DialectCapabilities:
    _capabilities: CapabilityDict = {}

    @classmethod
    def capabilities(cls) -> CapabilityDict:
        return cls._capabilities

    @classmethod
    def supports(cls, capability: Capability) -> bool:
        return bool(cls.capabilities()[capability])