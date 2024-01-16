from collections.abc import Hashable
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterator,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from dbt_common.exceptions import CompilationError, DbtRuntimeError
from dbt_common.utils import deep_merge, filter_null_values

from dbt.adapters.contracts.relation import (
    ComponentName,
    HasQuoting,
    FakeAPIObject,
    Path,
    Policy,
    RelationConfig,
    RelationType,
)
from dbt.adapters.exceptions import (
    ApproximateMatchError,
    MultipleDatabasesNotAllowedError,
)
from dbt.adapters.utils import classproperty


Self = TypeVar("Self", bound="BaseRelation")
SerializableIterable = Union[Tuple, FrozenSet]


@dataclass(frozen=True, eq=False, repr=False)
class BaseRelation(FakeAPIObject, Hashable):
    path: Path
    type: Optional[RelationType] = None
    quote_character: str = '"'
    # Python 3.11 requires that these use default_factory instead of simple default
    # ValueError: mutable default <class 'dbt.contracts.relation.Policy'> for field include_policy is not allowed: use default_factory
    include_policy: Policy = field(default_factory=lambda: Policy())
    quote_policy: Policy = field(default_factory=lambda: Policy())
    dbt_created: bool = False
    limit: Optional[int] = None

    # register relation types that can be renamed for the purpose of replacing relations using stages and backups
    # adding a relation type here also requires defining the associated rename macro
    # e.g. adding RelationType.View in dbt-postgres requires that you define:
    # include/postgres/macros/relations/view/rename.sql::postgres__get_rename_view_sql()
    renameable_relations: SerializableIterable = ()

    # register relation types that are atomically replaceable, e.g. they have "create or replace" syntax
    # adding a relation type here also requires defining the associated replace macro
    # e.g. adding RelationType.View in dbt-postgres requires that you define:
    # include/postgres/macros/relations/view/replace.sql::postgres__get_replace_view_sql()
    replaceable_relations: SerializableIterable = ()

    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        if self.dbt_created and self.quote_policy.get_part(field) is False:
            return self.path.get_lowered_part(field) == value.lower()
        else:
            return self.path.get_part(field) == value

    @classmethod
    def _get_field_named(cls, field_name):
        for f, _ in cls._get_fields():
            if f.name == field_name:
                return f
        # this should be unreachable
        raise ValueError(f"BaseRelation has no {field_name} field!")

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict(omit_none=True) == other.to_dict(omit_none=True)

    @classmethod
    def get_default_quote_policy(cls) -> Policy:
        return cls._get_field_named("quote_policy").default_factory()

    @classmethod
    def get_default_include_policy(cls) -> Policy:
        return cls._get_field_named("include_policy").default_factory()

    def get(self, key, default=None):
        """Override `.get` to return a metadata object so we don't break
        dbt_utils.
        """
        if key == "metadata":
            return {"type": self.__class__.__name__}
        return super().get(key, default)

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        if not search:
            # nothing was passed in
            raise DbtRuntimeError("Tried to match relation, but no search path was passed!")

        exact_match = True
        approximate_match = True

        for k, v in search.items():
            if not self._is_exactish_match(k, v):
                exact_match = False
            if str(self.path.get_lowered_part(k)).strip(self.quote_character) != v.lower().strip(
                self.quote_character
            ):
                approximate_match = False  # type: ignore[union-attr]

        if approximate_match and not exact_match:
            target = self.create(database=database, schema=schema, identifier=identifier)
            raise ApproximateMatchError(target, self)

        return exact_match

    def replace_path(self, **kwargs):
        return self.replace(path=self.path.replace(**kwargs))

    def quote(
        self: Self,
        database: Optional[bool] = None,
        schema: Optional[bool] = None,
        identifier: Optional[bool] = None,
    ) -> Self:
        policy = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        new_quote_policy = self.quote_policy.replace_dict(policy)
        return self.replace(quote_policy=new_quote_policy)

    def include(
        self: Self,
        database: Optional[bool] = None,
        schema: Optional[bool] = None,
        identifier: Optional[bool] = None,
    ) -> Self:
        policy = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        new_include_policy = self.include_policy.replace_dict(policy)
        return self.replace(include_policy=new_include_policy)

    def information_schema(self, view_name=None) -> "InformationSchema":
        # some of our data comes from jinja, where things can be `Undefined`.
        if not isinstance(view_name, str):
            view_name = None

        # Kick the user-supplied schema out of the information schema relation
        # Instead address this as <database>.information_schema by default
        info_schema = InformationSchema.from_relation(self, view_name)
        return info_schema.incorporate(path={"schema": None})

    def information_schema_only(self) -> "InformationSchema":
        return self.information_schema()

    def without_identifier(self) -> "BaseRelation":
        """Return a form of this relation that only has the database and schema
        set to included. To get the appropriately-quoted form the schema out of
        the result (for use as part of a query), use `.render()`. To get the
        raw database or schema name, use `.database` or `.schema`.

        The hash of the returned object is the result of render().
        """
        return self.include(identifier=False).replace_path(identifier=None)

    def _render_iterator(
        self,
    ) -> Iterator[Tuple[Optional[ComponentName], Optional[str]]]:
        for key in ComponentName:
            path_part: Optional[str] = None
            if self.include_policy.get_part(key):
                path_part = self.path.get_part(key)
                if path_part is not None and self.quote_policy.get_part(key):
                    path_part = self.quoted(path_part)
            yield key, path_part

    def render(self) -> str:
        # if there is nothing set, this will return the empty string.
        return ".".join(part for _, part in self._render_iterator() if part is not None)

    def render_limited(self) -> str:
        rendered = self.render()
        if self.limit is None:
            return rendered
        elif self.limit == 0:
            return f"(select * from {rendered} where false limit 0) _dbt_limit_subq"
        else:
            return f"(select * from {rendered} limit {self.limit}) _dbt_limit_subq"

    def quoted(self, identifier):
        return "{quote_char}{identifier}{quote_char}".format(
            quote_char=self.quote_character,
            identifier=identifier,
        )

    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f"__dbt__cte__{name}"

    @classmethod
    def create_ephemeral_from(
        cls: Type[Self],
        relation_config: RelationConfig,
        limit: Optional[int],
    ) -> Self:
        # Note that ephemeral models are based on the name.
        identifier = cls.add_ephemeral_prefix(relation_config.name)
        return cls.create(
            type=cls.CTE,
            identifier=identifier,
            limit=limit,
        ).quote(identifier=False)

    @classmethod
    def create_from(
        cls: Type[Self],
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs: Any,
    ) -> Self:
        quote_policy = kwargs.pop("quote_policy", {})

        config_quoting = relation_config.quoting_dict
        config_quoting.pop("column", None)
        # precedence: kwargs quoting > relation config quoting > base quoting > default quoting
        quote_policy = deep_merge(
            cls.get_default_quote_policy().to_dict(omit_none=True),
            quoting.quoting,
            config_quoting,
            quote_policy,
        )

        return cls.create(
            database=relation_config.database,
            schema=relation_config.schema,
            identifier=relation_config.identifier,
            quote_policy=quote_policy,
            **kwargs,
        )

    @classmethod
    def create(
        cls: Type[Self],
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
        type: Optional[RelationType] = None,
        **kwargs,
    ) -> Self:
        kwargs.update(
            {
                "path": {
                    "database": database,
                    "schema": schema,
                    "identifier": identifier,
                },
                "type": type,
            }
        )
        return cls.from_dict(kwargs)

    @property
    def can_be_renamed(self) -> bool:
        return self.type in self.renameable_relations

    @property
    def can_be_replaced(self) -> bool:
        return self.type in self.replaceable_relations

    def __repr__(self) -> str:
        return "<{} {}>".format(self.__class__.__name__, self.render())

    def __hash__(self) -> int:
        return hash(self.render())

    def __str__(self) -> str:
        return self.render() if self.limit is None else self.render_limited()

    @property
    def database(self) -> Optional[str]:
        return self.path.database

    @property
    def schema(self) -> Optional[str]:
        return self.path.schema

    @property
    def identifier(self) -> Optional[str]:
        return self.path.identifier

    @property
    def table(self) -> Optional[str]:
        return self.path.identifier

    # Here for compatibility with old Relation interface
    @property
    def name(self) -> Optional[str]:
        return self.identifier

    @property
    def is_table(self) -> bool:
        return self.type == RelationType.Table

    @property
    def is_cte(self) -> bool:
        return self.type == RelationType.CTE

    @property
    def is_view(self) -> bool:
        return self.type == RelationType.View

    @property
    def is_materialized_view(self) -> bool:
        return self.type == RelationType.MaterializedView

    @classproperty
    def Table(cls) -> str:
        return str(RelationType.Table)

    @classproperty
    def CTE(cls) -> str:
        return str(RelationType.CTE)

    @classproperty
    def View(cls) -> str:
        return str(RelationType.View)

    @classproperty
    def External(cls) -> str:
        return str(RelationType.External)

    @classproperty
    def MaterializedView(cls) -> str:
        return str(RelationType.MaterializedView)

    @classproperty
    def get_relation_type(cls) -> Type[RelationType]:
        return RelationType


Info = TypeVar("Info", bound="InformationSchema")


@dataclass(frozen=True, eq=False, repr=False)
class InformationSchema(BaseRelation):
    information_schema_view: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.information_schema_view, (type(None), str)):
            raise CompilationError("Got an invalid name: {}".format(self.information_schema_view))

    @classmethod
    def get_path(cls, relation: BaseRelation, information_schema_view: Optional[str]) -> Path:
        return Path(
            database=relation.database,
            schema=relation.schema,
            identifier="INFORMATION_SCHEMA",
        )

    @classmethod
    def get_include_policy(
        cls,
        relation,
        information_schema_view: Optional[str],
    ) -> Policy:
        return relation.include_policy.replace(
            database=relation.database is not None,
            schema=False,
            identifier=True,
        )

    @classmethod
    def get_quote_policy(
        cls,
        relation,
        information_schema_view: Optional[str],
    ) -> Policy:
        return relation.quote_policy.replace(
            identifier=False,
        )

    @classmethod
    def from_relation(
        cls: Type[Info],
        relation: BaseRelation,
        information_schema_view: Optional[str],
    ) -> Info:
        include_policy = cls.get_include_policy(relation, information_schema_view)
        quote_policy = cls.get_quote_policy(relation, information_schema_view)
        path = cls.get_path(relation, information_schema_view)
        return cls(
            type=RelationType.View,
            path=path,
            include_policy=include_policy,
            quote_policy=quote_policy,
            information_schema_view=information_schema_view,
        )

    def _render_iterator(self):
        for k, v in super()._render_iterator():
            yield k, v
        yield None, self.information_schema_view


class SchemaSearchMap(Dict[InformationSchema, Set[Optional[str]]]):
    """A utility class to keep track of what information_schema tables to
    search for what schemas. The schema values are all lowercased to avoid
    duplication.
    """

    def add(self, relation: BaseRelation):
        key = relation.information_schema_only()
        if key not in self:
            self[key] = set()
        schema: Optional[str] = None
        if relation.schema is not None:
            schema = relation.schema.lower()
        self[key].add(schema)

    def search(self) -> Iterator[Tuple[InformationSchema, Optional[str]]]:
        for information_schema, schemas in self.items():
            for schema in schemas:
                yield information_schema, schema

    def flatten(self, allow_multiple_databases: bool = False) -> "SchemaSearchMap":
        new = self.__class__()

        # make sure we don't have multiple databases if allow_multiple_databases is set to False
        if not allow_multiple_databases:
            seen = {r.database.lower() for r in self if r.database}
            if len(seen) > 1:
                raise MultipleDatabasesNotAllowedError(seen)

        for information_schema_name, schema in self.search():
            path = {"database": information_schema_name.database, "schema": schema}
            new.add(
                information_schema_name.incorporate(
                    path=path,
                    quote_policy={"database": False},
                    include_policy={"database": False},
                )
            )

        return new
