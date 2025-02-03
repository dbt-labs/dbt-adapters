import abc
from typing import Mapping, Any


class AdapterHooks(abc.ABC):
    def pre_model_hook(self, config: Mapping[str, Any]) -> Any:
        """A hook for running some operation before the model materialization
        runs. The hook can assume it has a connection available.

        The only parameter is a configuration dictionary (the same one
        available in the materialization context). It should be considered
        read-only.

        The pre-model hook may return anything as a context, which will be
        passed to the post-model hook.
        """
        ...

    def post_model_hook(self, config: Mapping[str, Any], context: Any) -> None:
        """A hook for running some operation after the model materialization
        runs. The hook can assume it has a connection available.

        The first parameter is a configuration dictionary (the same one
        available in the materialization context). It should be considered
        read-only.

        The second parameter is the value returned by pre_mdoel_hook.
        """
        ...