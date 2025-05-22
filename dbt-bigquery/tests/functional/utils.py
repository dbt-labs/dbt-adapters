from datetime import datetime
import random
from typing import Optional

from dbt.tests.util import (
    get_connection,
    get_model_file,
    relation_from_name,
    set_model_file,
)


def query_relation_type(project, name: str) -> Optional[str]:
    relation = relation_from_name(project.adapter, name)

    with get_connection(project.adapter):
        results = project.adapter.get_bq_table(relation)

    assert results is not None, f"Relation {relation} not found"
    return results.table_type


def unique_prefix():
    # create a directory name that will be unique per test session
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)
    _runtime = (int(_runtime_timedelta.total_seconds() * 1e6)) + _runtime_timedelta.microseconds
    return f"test{_runtime}{_randint:04}"


def update_model(project, name: str, model: str) -> str:
    relation = relation_from_name(project.adapter, name)
    original_model = get_model_file(project, relation)
    set_model_file(project, relation, model)
    return original_model
