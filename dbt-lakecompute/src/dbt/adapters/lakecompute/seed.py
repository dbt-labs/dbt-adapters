"""Seed loading: rows rendered as literal ``insert ... values`` statements.

The driver has no parameter bindings and no bulk ingest, so a seed's rows are rendered into the
SQL text client-side, the way v2's ``execute_seed_on_compute_platform`` does. Each statement is
one HTTP request, so batches are capped by size as well as by row count.

Kept in its own module so it can be unit-tested without an adapter or a connection.
"""

from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Iterator, List, Sequence

#: v2's ``SEED_INSERT_BATCH_MAX_ROWS``
MAX_ROWS_PER_INSERT = 500

#: v2's ``SEED_INSERT_BATCH_MAX_BYTES``. The service caps a request body at 2 MiB; a batch is
#: flushed once its SQL text crosses this, whatever its row count.
MAX_BYTES_PER_INSERT = 1_000_000


def render_literal(value: Any) -> str:
    """Render one agate cell as a DuckDB SQL literal.

    A seed's cells are only ever ``None``, ``bool``, ``Decimal``, ``date``, ``datetime`` or ``str``.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        # fixed-point, so 1E+3 is written 1000; bare NaN / Infinity would be read as column names
        return format(value, "f") if value.is_finite() else f"'{value}'::double"
    # covers datetime, which is a date
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


def insert_statements(
    relation: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    max_rows: int = MAX_ROWS_PER_INSERT,
    max_bytes: int = MAX_BYTES_PER_INSERT,
) -> Iterator[str]:
    """Yield ``insert into relation (columns) values ...`` statements covering ``rows``.

    A batch ends at ``max_rows`` rows, or before the row that would take it past ``max_bytes``.
    The first row of a batch is always kept, so one oversized row cannot stall the load.
    """
    prefix = f"insert into {relation} ({', '.join(columns)}) values "
    prefix_size = len(prefix.encode())
    batch: List[str] = []
    size = prefix_size
    for row in rows:
        tuple_sql = "(" + ", ".join(render_literal(value) for value in row) + ")"
        tuple_size = len(tuple_sql.encode())
        added = tuple_size + (2 if batch else 0)
        if batch and (len(batch) >= max_rows or size + added > max_bytes):
            yield prefix + ", ".join(batch)
            batch, size = [], prefix_size
            added = tuple_size
        batch.append(tuple_sql)
        size += added
    if batch:
        yield prefix + ", ".join(batch)
