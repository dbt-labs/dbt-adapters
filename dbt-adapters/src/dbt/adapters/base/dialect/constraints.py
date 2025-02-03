from enum import Enum
from typing import Dict, Any, Optional, Union, List

from dbt.adapters.base import available
from dbt.adapters.events.types import ConstraintNotSupported, ConstraintNotEnforced
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType, ModelLevelConstraint
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import DbtValidationError


class ConstraintSupport(str, Enum):
    ENFORCED = "enforced"
    NOT_ENFORCED = "not_enforced"
    NOT_SUPPORTED = "not_supported"


class Constraints:

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    @classmethod
    def _parse_column_constraint(cls, raw_constraint: Dict[str, Any]) -> ColumnLevelConstraint:
        try:
            ColumnLevelConstraint.validate(raw_constraint)
            return ColumnLevelConstraint.from_dict(raw_constraint)
        except Exception:
            raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        """Render the given constraint as DDL text. Should be overriden by adapters which need custom constraint
        rendering."""
        constraint_expression = constraint.expression or ""

        rendered_column_constraint = None
        if constraint.type == ConstraintType.check and constraint_expression:
            rendered_column_constraint = f"check ({constraint_expression})"
        elif constraint.type == ConstraintType.not_null:
            rendered_column_constraint = f"not null {constraint_expression}"
        elif constraint.type == ConstraintType.unique:
            rendered_column_constraint = f"unique {constraint_expression}"
        elif constraint.type == ConstraintType.primary_key:
            rendered_column_constraint = f"primary key {constraint_expression}"
        elif constraint.type == ConstraintType.foreign_key:
            if constraint.to and constraint.to_columns:
                rendered_column_constraint = (
                    f"references {constraint.to} ({', '.join(constraint.to_columns)})"
                )
            elif constraint_expression:
                rendered_column_constraint = f"references {constraint_expression}"
        elif constraint.type == ConstraintType.custom and constraint_expression:
            rendered_column_constraint = constraint_expression

        if rendered_column_constraint:
            rendered_column_constraint = rendered_column_constraint.strip()

        return rendered_column_constraint

    @available
    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_column_constraints = []

        for v in raw_columns.values():
            col_name = cls.quote(v["name"]) if v.get("quote") else v["name"]
            rendered_column_constraint = [f"{col_name} {v['data_type']}"]
            for con in v.get("constraints", None):
                constraint = cls._parse_column_constraint(con)
                c = cls.process_parsed_constraint(constraint, cls.render_column_constraint)
                if c is not None:
                    rendered_column_constraint.append(c)
            rendered_column_constraints.append(" ".join(rendered_column_constraint))

        return rendered_column_constraints

    @classmethod
    def process_parsed_constraint(
            cls,
            parsed_constraint: Union[ColumnLevelConstraint, ModelLevelConstraint],
            render_func,
    ) -> Optional[str]:
        # skip checking enforcement if this is a 'custom' constraint
        if parsed_constraint.type == ConstraintType.custom:
            return render_func(parsed_constraint)
        if (
                parsed_constraint.warn_unsupported
                and cls.CONSTRAINT_SUPPORT[parsed_constraint.type] == ConstraintSupport.NOT_SUPPORTED
        ):
            warn_or_error(
                ConstraintNotSupported(constraint=parsed_constraint.type.value, adapter=cls.type())
            )
        if (
                parsed_constraint.warn_unenforced
                and cls.CONSTRAINT_SUPPORT[parsed_constraint.type] == ConstraintSupport.NOT_ENFORCED
        ):
            warn_or_error(
            ConstraintNotEnforced(constraint=parsed_constraint.type.value, adapter=cls.type())
            )
        if cls.CONSTRAINT_SUPPORT[parsed_constraint.type] != ConstraintSupport.NOT_SUPPORTED:
            return render_func(parsed_constraint)

        return None

    @classmethod
    def _parse_model_constraint(cls, raw_constraint: Dict[str, Any]) -> ModelLevelConstraint:
        try:
            ModelLevelConstraint.validate(raw_constraint)
            c = ModelLevelConstraint.from_dict(raw_constraint)
            return c
        except Exception:
            raise DbtValidationError(f"Could not parse constraint: {raw_constraint}")

    @available
    @classmethod
    def render_raw_model_constraints(cls, raw_constraints: List[Dict[str, Any]]) -> List[str]:
        return [c for c in map(cls.render_raw_model_constraint, raw_constraints) if c is not None]

    @classmethod
    def render_raw_model_constraint(cls, raw_constraint: Dict[str, Any]) -> Optional[str]:
        constraint = cls._parse_model_constraint(raw_constraint)
        return cls.process_parsed_constraint(constraint, cls.render_model_constraint)

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        """Render the given constraint as DDL text. Should be overriden by adapters which need custom constraint
        rendering."""
        constraint_prefix = f"constraint {constraint.name} " if constraint.name else ""
        column_list = ", ".join(constraint.columns)
        rendered_model_constraint = None

        if constraint.type == ConstraintType.check and constraint.expression:
            rendered_model_constraint = f"{constraint_prefix}check ({constraint.expression})"
        elif constraint.type == ConstraintType.unique:
            constraint_expression = f" {constraint.expression}" if constraint.expression else ""
            rendered_model_constraint = (
                f"{constraint_prefix}unique{constraint_expression} ({column_list})"
            )
        elif constraint.type == ConstraintType.primary_key:
            constraint_expression = f" {constraint.expression}" if constraint.expression else ""
            rendered_model_constraint = (
                f"{constraint_prefix}primary key{constraint_expression} ({column_list})"
            )
        elif constraint.type == ConstraintType.foreign_key:
            if constraint.to and constraint.to_columns:
                rendered_model_constraint = f"{constraint_prefix}foreign key ({column_list}) references {constraint.to} ({', '.join(constraint.to_columns)})"
            elif constraint.expression:
                rendered_model_constraint = f"{constraint_prefix}foreign key ({column_list}) references {constraint.expression}"
        elif constraint.type == ConstraintType.custom and constraint.expression:
            rendered_model_constraint = f"{constraint_prefix}{constraint.expression}"

        return rendered_model_constraint
