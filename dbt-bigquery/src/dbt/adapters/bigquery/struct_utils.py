import copy
from typing import Any, Dict, List, Mapping, Sequence

from dbt.adapters.bigquery.column import BigQueryColumn


def merge_nested_fields(
    existing_fields: Sequence[Dict[str, Any]],
    additions: Mapping[str, Dict[str, Any]],
    prefix: str = "",
) -> List[Dict[str, Any]]:
    """Merge new fields into existing STRUCT fields, appending at each nesting level.

    Note: Primarily used for field removal. For adding fields, sync_struct_columns
    uses the source schema directly to preserve field order.
    """
    merged_fields: List[Dict[str, Any]] = []
    addition_lookup = dict(additions)

    for field in existing_fields:
        field_name = field["name"]
        qualified_name = f"{prefix}.{field_name}" if prefix else field_name

        direct_addition = addition_lookup.pop(qualified_name, None)
        if direct_addition is not None:
            merged_fields.append(copy.deepcopy(direct_addition))
            continue

        nested_additions = {
            key: value
            for key, value in list(addition_lookup.items())
            if key.startswith(f"{qualified_name}.")
        }

        if nested_additions and field.get("type") == "RECORD":
            for key in nested_additions:
                addition_lookup.pop(key, None)

            stripped_additions = {
                key.split(".", 1)[1]: value for key, value in nested_additions.items()
            }

            merged_children = merge_nested_fields(
                field.get("fields", []) or [],
                stripped_additions,
                prefix="",
            )

            merged_field = copy.deepcopy(field)
            merged_field["fields"] = merged_children
            merged_fields.append(merged_field)
        else:
            merged_fields.append(copy.deepcopy(field))

    for path, addition in addition_lookup.items():
        if "." not in path:
            merged_fields.append(copy.deepcopy(addition))

    return merged_fields


def collect_field_dicts(
    fields: Sequence[Dict[str, Any]], prefix: str = ""
) -> Dict[str, Dict[str, Any]]:
    collected: Dict[str, Dict[str, Any]] = {}
    for field in fields:
        name = field["name"]
        path = f"{prefix}.{name}" if prefix else name
        collected[path] = field
        if field.get("type") == "RECORD":
            collected.update(collect_field_dicts(field.get("fields", []) or [], path))
    return collected


def find_missing_fields(
    source_fields: Sequence[Dict[str, Any]],
    target_fields: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    source_map = collect_field_dicts(source_fields)
    target_map = collect_field_dicts(target_fields)
    return {
        path: copy.deepcopy(field) for path, field in source_map.items() if path not in target_map
    }


def build_nested_additions(add_columns: Sequence[BigQueryColumn]) -> Dict[str, Dict[str, Any]]:
    additions: Dict[str, Dict[str, Any]] = {}

    for column in add_columns:
        schema_field = column.column_to_bq_schema().to_api_repr()
        additions[column.name] = schema_field

    return additions
