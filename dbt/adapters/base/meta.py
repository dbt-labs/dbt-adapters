import abc
from collections import defaultdict
import contextvars
import json
import os
from functools import wraps
from typing import Any, Callable, Dict, FrozenSet, Optional, Set
import os.path
import uuid

from dbt_common.events.functions import warn_or_error

from dbt.adapters.events.types import AdapterDeprecationWarning
from dbt.adapters.contracts.connection import AdapterResponse
import agate
from dbt.adapters.base.adapter_function_recording_pb2 import (
    Object,
    AdapterResponse as PbAdapterResponse,
    AgateTable as PbAgateTable,
    PostgresRelation as PbPostgresRelation,
    NullValue,
    RecordingData,
    ThreadOperations,
    OperationList,
    Operation,
)


Decorator = Callable[[Any], Callable]


# start a context var to store the thread name
thread_name = contextvars.ContextVar("thread_name", default="master")


# define functions to set and get the thread name
def set_thread_name(name: str):
    thread_name.set(name)


def get_thread_name():
    return thread_name.get()


# start a recorder to record the function calls, it takes a folder path
# as input, it will record function calls for each thread in a folder named after the thread name
# the folder will be created if it does not exist
# the recorder will record the function name, args, kwargs, and return value
# each function will be a single json file in the folder with schema  {operation:[{input_args:xxxx, output: xxx, error: xxx}]}
# the recorder will also record the thread name in the folder
class Recorder:
    def __init__(self, folder_path: str):
        self.folder_path = folder_path
        os.makedirs(self.folder_path, exist_ok=True)
        self.operations: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))

    def call_and_record(self, func: Callable, *args, **kwargs):
        # get the thread name
        thread_name = get_thread_name()
        # create a folder for the thread if it does not exist
        # record the function call
        err = None
        try:
            ret = func(*args, **kwargs)
            operation = {
                "function_name": func.__name__,
                "input_args": args,
                "input_kwargs": kwargs,
                "output": ret,
                "error": None,
            }
        except Exception as e:
            operation = {
                "function_name": func.__name__,
                "input_args": args,
                "input_kwargs": kwargs,
                "output": None,
                "error": str(e),
            }
            err = e
        finally:
            self.operations[thread_name][func.__name__].append(operation)
            if err:
                raise err
        return ret

    def save(self):

        def to_write_obj(value, thread_folder) -> Object:
            from dbt.adapters.postgres import PostgresRelation

            result = Object()
            if isinstance(value, AdapterResponse):
                adapter_response = PbAdapterResponse(
                    message=str(getattr(value, "message", "")),
                    code=str(getattr(value, "code", "")),
                    rows_affected=getattr(value, "rows_affected", 0),
                    query_id=getattr(value, "query_id", None),
                )
                result.adapter_response.CopyFrom(adapter_response)
            elif isinstance(value, agate.Table):
                id = str(uuid.uuid4())
                # Convert table rows to list of dicts
                rows = [dict(zip(value.column_names, row)) for row in value.rows]
                with open(os.path.join(thread_folder, f"{id}.json"), "w") as f:
                    json.dump(rows, f)
                agate_table = PbAgateTable(table_id=id)
                result.agate_table.CopyFrom(agate_table)
            elif isinstance(value, PostgresRelation):
                postgres_relation = PbPostgresRelation()
                postgres_relation.data.update(value.to_dict())
                result.postgres_relation.CopyFrom(postgres_relation)
            elif value is None:
                result.null_value = NullValue.NULL_VALUE
            elif isinstance(value, str):
                result.string_value = value
            elif isinstance(value, bool):
                result.boolean_value = value
            else:
                raise ValueError(f"Unknown result type: {type(value)}")
            return result

        recording_data = RecordingData()

        for thread_name, operations in self.operations.items():
            thread_folder = os.path.join(self.folder_path, thread_name)
            os.makedirs(thread_folder, exist_ok=True)

            for operation_name, operation_list in operations.items():
                # Create a new recording data object for each operation type
                recording_data = RecordingData()
                thread_ops = ThreadOperations()
                op_list = OperationList()

                # Process all operations of this type
                for op in operation_list:
                    operation = Operation()
                    operation.function_name = op["function_name"]

                    # Convert input args (skip self)
                    for arg in op["input_args"][1:]:
                        result = operation.input_args.add()
                        result.CopyFrom(to_write_obj(arg, thread_folder))

                    # Convert input kwargs
                    for k, v in op["input_kwargs"].items():
                        operation.input_kwargs[k].CopyFrom(to_write_obj(v, thread_folder))

                    # Convert outputs
                    if isinstance(op["output"], (list, tuple)):
                        for out in op["output"]:
                            result = operation.outputs.add()
                            result.CopyFrom(to_write_obj(out, thread_folder))
                    elif op["output"] is not None:
                        result = operation.outputs.add()
                        result.CopyFrom(to_write_obj(op["output"], thread_folder))

                    # Add error if present
                    if op["error"]:
                        operation.error = op["error"]

                    op_list.operations.append(operation)

                thread_ops.operations[operation_name].CopyFrom(op_list)
                recording_data.thread_operations[thread_name].CopyFrom(thread_ops)

                # Write all operations of same type to a single pb file
                print(f"Writing {operation_name} to {thread_folder}")
                pb_filename = f"{operation_name}.pb"
                with open(os.path.join(thread_folder, pb_filename), "wb") as f:
                    f.write(recording_data.SerializeToString())


recorder = Recorder("/Users/chenyuli/git/recording_test_simple")


def record_function(func: Callable):
    def wrapper(*args, **kwargs):
        return recorder.call_and_record(func, *args, **kwargs)

    return wrapper


class _Available:
    def __call__(self, func: Callable) -> Callable:
        func._is_available_ = True  # type: ignore
        return func

    def parse(self, parse_replacement: Callable) -> Decorator:
        """A decorator factory to indicate that a method on the adapter will be
        exposed to the database wrapper, and will be stubbed out at parse time
        with the given function.

        @available.parse()
        def my_method(self, a, b):
            if something:
                return None
            return big_expensive_db_query()

        @available.parse(lambda *args, **args: {})
        def my_other_method(self, a, b):
            x = {}
            x.update(big_expensive_db_query())
            return x
        """

        def inner(func):
            func._parse_replacement_ = parse_replacement
            return self(func)

        return inner

    def deprecated(
        self, supported_name: str, parse_replacement: Optional[Callable] = None
    ) -> Decorator:
        """A decorator that marks a function as available, but also prints a
        deprecation warning. Use like

        @available.deprecated('my_new_method')
        def my_old_method(self, arg):
            args = compatability_shim(arg)
            return self.my_new_method(*args)

        @available.deprecated('my_new_slow_method', lambda *a, **k: (0, ''))
        def my_old_slow_method(self, arg):
            args = compatibility_shim(arg)
            return self.my_new_slow_method(*args)

        To make `adapter.my_old_method` available but also print out a warning
        on use directing users to `my_new_method`.

        The optional parse_replacement, if provided, will provide a parse-time
        replacement for the actual method (see `available.parse`).
        """

        def wrapper(func):
            func_name = func.__name__

            @wraps(func)
            def inner(*args, **kwargs):
                warn_or_error(
                    AdapterDeprecationWarning(old_name=func_name, new_name=supported_name)
                )
                return func(*args, **kwargs)

            if parse_replacement:
                available_function = self.parse(parse_replacement)
            else:
                available_function = self
            return available_function(inner)

        return wrapper

    def parse_none(self, func: Callable) -> Callable:
        wrapper = self.parse(lambda *a, **k: None)
        return wrapper(func)

    def parse_list(self, func: Callable) -> Callable:
        wrapper = self.parse(lambda *a, **k: [])
        return wrapper(func)


available = _Available()


class available_property(property):
    """
    This supports making dynamic properties (`@property`) available in the jinja context.

    We use `@available` to make methods available in the jinja context, but this mechanism relies on the method being callable.
    Intuitively, we should be able to use both `@available` and `@property` to create a dynamic property that's available in the jinja context.

    Using the `@property` decorator as the inner decorator supplies `@available` with something that is not callable.
    Instead of returning the method, `@property` returns the value itself, not the method that is called to create the value.

    Using the `@available` decorator as the inner decorator adds `_is_available_ = True` to the function.
    However, when the `@property` decorator executes, it returns a `property` object which does not have the `_is_available_` attribute.

    This decorator solves this problem by simply adding `_is_available_ = True` as an attribute on the `property` built-in.
    """

    _is_available_ = True


class AdapterMeta(abc.ABCMeta):
    _available_: FrozenSet[str]
    _parse_replacements_: Dict[str, Callable]

    def __new__(mcls, name, bases, namespace, **kwargs) -> "AdapterMeta":
        # mypy does not like the `**kwargs`. But `ABCMeta` itself takes
        # `**kwargs` in its argspec here (and passes them to `type.__new__`.
        # I'm not sure there is any benefit to it after poking around a bit,
        # but having it doesn't hurt on the python side (and omitting it could
        # hurt for obscure metaclass reasons, for all I know)
        cls = abc.ABCMeta.__new__(mcls, name, bases, namespace, **kwargs)  # type: ignore

        # this is very much inspired by ABCMeta's own implementation

        # dict mapping the method name to whether the model name should be
        # injected into the arguments. All methods in here are exposed to the
        # context.
        available: Set[str] = set()
        replacements: Dict[str, Any] = {}

        # collect base class data first
        for base in bases:
            available.update(getattr(base, "_available_", set()))
            replacements.update(getattr(base, "_parse_replacements_", set()))

        # override with local data if it exists
        for name, value in namespace.items():
            if getattr(value, "_is_available_", False):
                available.add(name)
            parse_replacement = getattr(value, "_parse_replacement_", None)
            if parse_replacement is not None:
                replacements[name] = parse_replacement

        cls._available_ = frozenset(available)
        # should this be a namedtuple so it will be immutable like _available_?
        cls._parse_replacements_ = replacements
        return cls
