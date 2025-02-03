from typing import Optional

from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.protocol import MacroContextGeneratorCallable


class MacroHandler:
    _macro_resolver: Optional[MacroResolverProtocol]

    def set_macro_resolver(self, macro_resolver: MacroResolverProtocol) -> None:
        self._macro_resolver = macro_resolver

    def get_macro_resolver(self) -> Optional[MacroResolverProtocol]:
        return self._macro_resolver

    def clear_macro_resolver(self) -> None:
        if self._macro_resolver is not None:
            self._macro_resolver = None

    def set_macro_context_generator(
            self,
            macro_context_generator: MacroContextGeneratorCallable,
    ) -> None:
        self._macro_context_generator = macro_context_generator