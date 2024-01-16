import abc
from dataclasses import dataclass, field
import itertools
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

from dbt_common.contracts.util import Replaceable
from dbt_common.dataclass_schema import (
    ExtensibleDbtClassMixin,
    StrEnum,
    ValidatedStringMixin,
    dbtClassMixin,
)

# TODO: this is a very bad dependency - shared global state
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtInternalError
from dbt_common.utils import md5
from mashumaro.jsonschema.annotations import Pattern
from typing_extensions import Protocol, Annotated

from dbt.adapters.events.types import NewConnectionOpening
from dbt.adapters.utils import translate_aliases


class Identifier(ValidatedStringMixin):
    ValidationRegex = r"^[A-Za-z_][A-Za-z0-9_]+$"


@dataclass
class AdapterResponse(dbtClassMixin):
    _message: str
    code: Optional[str] = None
    rows_affected: Optional[int] = None

    def __str__(self):
        return self._message


class ConnectionState(StrEnum):
    INIT = "init"
    OPEN = "open"
    CLOSED = "closed"
    FAIL = "fail"


@dataclass(init=False)
class Connection(ExtensibleDbtClassMixin, Replaceable):
    # Annotated is used by mashumaro for jsonschema generation
    type: Annotated[Identifier, Pattern(r"^[A-Za-z_][A-Za-z0-9_]+$")]
    name: Optional[str] = None
    state: ConnectionState = ConnectionState.INIT
    transaction_open: bool = False
    _handle: Optional[Any] = None
    _credentials: Optional[Any] = None

    def __init__(
        self,
        type: Identifier,
        name: Optional[str],
        credentials: dbtClassMixin,
        state: ConnectionState = ConnectionState.INIT,
        transaction_open: bool = False,
        handle: Optional[Any] = None,
    ) -> None:
        self.type = type
        self.name = name
        self.state = state
        self.credentials = credentials
        self.transaction_open = transaction_open
        self.handle = handle

    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        self._credentials = value

    @property
    def handle(self):
        if isinstance(self._handle, LazyHandle):
            try:
                # this will actually change 'self._handle'.
                self._handle.resolve(self)
            except RecursionError as exc:
                raise DbtInternalError(
                    "A connection's open() method attempted to read the handle value"
                ) from exc
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value


class LazyHandle:
    """The opener must be a callable that takes a Connection object and opens the
    connection, updating the handle on the Connection.
    """

    def __init__(self, opener: Callable[[Connection], Connection]) -> None:
        self.opener = opener

    def resolve(self, connection: Connection) -> Connection:
        fire_event(
            NewConnectionOpening(connection_state=connection.state, node_info=get_node_info())
        )
        return self.opener(connection)


# see https://github.com/python/mypy/issues/4717#issuecomment-373932080
# and https://github.com/python/mypy/issues/5374
# for why we have type: ignore. Maybe someday dataclasses + abstract classes
# will work.
@dataclass  # type: ignore
class Credentials(ExtensibleDbtClassMixin, Replaceable, metaclass=abc.ABCMeta):
    database: str
    schema: str
    _ALIASES: ClassVar[Dict[str, str]] = field(default={}, init=False)

    @abc.abstractproperty
    def type(self) -> str:
        raise NotImplementedError("type not implemented for base credentials class")

    @property
    def unique_field(self) -> str:
        """Hashed and included in anonymous telemetry to track adapter adoption.
        Return the field from Credentials that can uniquely identify
        one team/organization using this adapter
        """
        raise NotImplementedError("unique_field not implemented for base credentials class")

    def hashed_unique_field(self) -> str:
        return md5(self.unique_field)

    def connection_info(self, *, with_aliases: bool = False) -> Iterable[Tuple[str, Any]]:
        """Return an ordered iterator of key/value pairs for pretty-printing."""
        as_dict = self.to_dict(omit_none=False)
        connection_keys = set(self._connection_keys())
        aliases: List[str] = []
        if with_aliases:
            aliases = [k for k, v in self._ALIASES.items() if v in connection_keys]
        for key in itertools.chain(self._connection_keys(), aliases):
            if key in as_dict:
                yield key, as_dict[key]

    @abc.abstractmethod
    def _connection_keys(self) -> Tuple[str, ...]:
        raise NotImplementedError

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        # Need to fixup dbname => database, pass => password
        data = cls.translate_aliases(data)
        return data

    @classmethod
    def translate_aliases(cls, kwargs: Dict[str, Any], recurse: bool = False) -> Dict[str, Any]:
        return translate_aliases(kwargs, cls._ALIASES, recurse)

    def __post_serialize__(self, dct):
        # no super() -- do we need it?
        if self._ALIASES:
            dct.update(
                {
                    new_name: dct[canonical_name]
                    for new_name, canonical_name in self._ALIASES.items()
                    if canonical_name in dct
                }
            )
        return dct


class HasCredentials(Protocol):
    credentials: Credentials
    profile_name: str
    target_name: str
    threads: int

    def to_target_dict(self):
        raise NotImplementedError("to_target_dict not implemented")


DEFAULT_QUERY_COMMENT = """
{%- set comment_dict = {} -%}
{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}
{%- if node is not none -%}
  {%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}
{% else %}
  {# in the node context, the connection name is the node_id #}
  {%- do comment_dict.update(connection_name=connection_name) -%}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
"""


@dataclass
class QueryComment(dbtClassMixin):
    comment: str = DEFAULT_QUERY_COMMENT
    append: bool = False
    job_label: bool = field(default=False, metadata={"alias": "job-label"})


class AdapterRequiredConfig(HasCredentials, Protocol):
    project_name: str
    query_comment: QueryComment
    cli_vars: Dict[str, Any]
    target_path: str
    log_cache_events: bool
