import logging
import re

from dbt_common.events.base_types import msg_from_base_event
from dbt_common.events.functions import msg_to_dict, msg_to_json

from dbt.adapters.events import types
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events import base_types


_BASE_EVENTS = [
    base_types.AdapterBaseEvent,
    base_types.DebugLevel,
    base_types.DynamicLevel,
    base_types.ErrorLevel,
    base_types.InfoLevel,
    base_types.TestLevel,
    base_types.WarnLevel,
]


# takes in a class and finds any subclasses for it
def get_all_subclasses(cls):
    all_subclasses = []
    for subclass in cls.__subclasses__():
        if subclass not in _BASE_EVENTS:
            all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))
    return set(all_subclasses)


class TestAdapterLogger:
    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_basic_adapter_logging_interface(self):
        logger = AdapterLogger("dbt_tests")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.exception("exception message")
        logger.critical("exception message")

    # python loggers allow deferring string formatting via this signature:
    def test_formatting(self):
        logger = AdapterLogger("dbt_tests")
        # tests that it doesn't throw
        logger.debug("hello {}", "world")

        # enters lower in the call stack to test that it formats correctly
        event = types.AdapterEventDebug(name="dbt_tests", base_msg="hello {}", args=["world"])
        assert "hello world" in event.message()

        # tests that it doesn't throw
        logger.debug("1 2 {}", "3")

        # enters lower in the call stack to test that it formats correctly
        event = types.AdapterEventDebug(name="dbt_tests", base_msg="1 2 {}", args=[3])
        assert "1 2 3" in event.message()

        # tests that it doesn't throw
        logger.debug("boop{x}boop")

        # enters lower in the call stack to test that it formats correctly
        # in this case it's that we didn't attempt to replace anything since there
        # were no args passed after the initial message
        event = types.AdapterEventDebug(name="dbt_tests", base_msg="boop{x}boop", args=[])
        assert "boop{x}boop" in event.message()

        # ensure AdapterLogger and subclasses makes all base_msg members
        # of type string; when someone writes logger.debug(a) where a is
        # any non-string object
        event = types.AdapterEventDebug(name="dbt_tests", base_msg=[1, 2, 3], args=[3])
        assert isinstance(event.base_msg, str)

    def test_set_adapter_dependency_log_level(self):
        logger = AdapterLogger("dbt_tests")
        package_log = logging.getLogger("test_package_log")
        logger.set_adapter_dependency_log_level("test_package_log", "DEBUG")
        package_log.debug("debug message")


class TestEventCodes:
    # checks to see if event codes are duplicated to keep codes singular and clear.
    # also checks that event codes follow correct naming convention ex. E001
    def test_event_codes(self):
        all_concrete = get_all_subclasses(base_types.AdapterBaseEvent)
        all_codes = set()

        for event_cls in all_concrete:
            code = event_cls.code(event_cls)
            # must be in the form 1 capital letter, 3 digits
            assert re.match("^[A-Z][0-9]{3}", code)
            # cannot have been used already
            assert (
                code not in all_codes
            ), f"{code} is assigned more than once. Check types.py for duplicates."
            all_codes.add(code)


sample_values = [
    # N.B. Events instantiated here include the module prefix in order to
    # avoid having the entire list twice in the code.
    # D - Deprecations ======================
    types.AdapterDeprecationWarning(old_name="", new_name=""),
    types.CollectFreshnessReturnSignature(),
    # E - DB Adapter ======================
    types.AdapterEventDebug(),
    types.AdapterEventInfo(),
    types.AdapterEventWarning(),
    types.AdapterEventError(),
    types.AdapterRegistered(adapter_name="dbt-awesome", adapter_version="1.2.3"),
    types.NewConnection(conn_type="", conn_name=""),
    types.ConnectionReused(conn_name=""),
    types.ConnectionLeftOpenInCleanup(conn_name=""),
    types.ConnectionClosedInCleanup(conn_name=""),
    types.RollbackFailed(conn_name=""),
    types.ConnectionClosed(conn_name=""),
    types.ConnectionLeftOpen(conn_name=""),
    types.Rollback(conn_name=""),
    types.CacheMiss(conn_name="", database="", schema=""),
    types.ListRelations(database="", schema=""),
    types.ConnectionUsed(conn_type="", conn_name=""),
    types.SQLQuery(conn_name="", sql=""),
    types.SQLQueryStatus(status="", elapsed=0.1),
    types.SQLCommit(conn_name=""),
    types.ColTypeChange(
        orig_type="",
        new_type="",
        table={"database": "", "schema": "", "identifier": ""},
    ),
    types.SchemaCreation(relation={"database": "", "schema": "", "identifier": ""}),
    types.SchemaDrop(relation={"database": "", "schema": "", "identifier": ""}),
    types.CacheAction(
        action="adding_relation",
        ref_key={"database": "", "schema": "", "identifier": ""},
        ref_key_2={"database": "", "schema": "", "identifier": ""},
    ),
    types.CacheDumpGraph(before_after="before", action="rename", dump=dict()),
    types.AdapterImportError(exc=""),
    types.PluginLoadError(exc_info=""),
    types.NewConnectionOpening(connection_state=""),
    types.CodeExecution(conn_name="", code_content=""),
    types.CodeExecutionStatus(status="", elapsed=0.1),
    types.CatalogGenerationError(exc=""),
    types.WriteCatalogFailure(num_exceptions=0),
    types.CatalogWritten(path=""),
    types.CannotGenerateDocs(),
    types.BuildingCatalog(),
    types.DatabaseErrorRunningHook(hook_type=""),
    types.HooksRunning(num_hooks=0, hook_type=""),
    types.FinishedRunningStats(stat_line="", execution="", execution_time=0),
    types.ConstraintNotEnforced(constraint="", adapter=""),
    types.ConstraintNotSupported(constraint="", adapter=""),
    types.TypeCodeNotFound(type_code=0),
]


class TestEventJSONSerialization:
    # attempts to test that every event is serializable to json.
    # event types that take `Any` are not possible to test in this way since some will serialize
    # just fine and others won't.
    def test_all_serializable(self):
        all_non_abstract_events = set(
            get_all_subclasses(base_types.AdapterBaseEvent),
        )
        all_event_values_list = list(map(lambda x: x.__class__, sample_values))
        diff = all_non_abstract_events.difference(set(all_event_values_list))
        assert not diff, (
            f"{diff}test is missing concrete values in `sample_values`. "
            f"Please add the values for the aforementioned event classes"
        )

        # make sure everything in the list is a value not a type
        for event in sample_values:
            assert not isinstance(event, type)

        # if we have everything we need to test, try to serialize everything
        count = 0
        for event in sample_values:
            msg = msg_from_base_event(event)
            print(f"--- msg: {msg.info.name}")
            # Serialize to dictionary
            try:
                msg_to_dict(msg)
            except Exception as e:
                raise Exception(
                    f"{event} can not be converted to a dict. Originating exception: {e}"
                )
            # Serialize to json
            try:
                msg_to_json(msg)
            except Exception as e:
                raise Exception(f"{event} is not serializable to json. Originating exception: {e}")
            # Serialize to binary
            try:
                msg.SerializeToString()
            except Exception as e:
                raise Exception(
                    f"{event} is not serializable to binary protobuf. Originating exception: {e}"
                )
            count += 1
        print(f"--- Found {count} events")
