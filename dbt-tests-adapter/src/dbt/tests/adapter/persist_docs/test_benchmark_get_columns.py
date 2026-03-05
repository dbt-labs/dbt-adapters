import time

import pytest

from dbt.tests.util import run_dbt

_COLUMN_COUNTS = [10, 50, 100, 500]
_ITERATIONS = 10


def _make_wide_model(n_cols):
    cols = ", ".join(f"1 as col_{i}" for i in range(n_cols))
    return f"""{{{{ config(materialized='table') }}}}
SELECT {cols}
"""


class BaseBenchmarkGetColumnsInRelation:
    """Benchmark adapter.get_columns_in_relation across different column counts.

    Measures the per-call latency that validate_doc_columns adds to the
    persist_docs flow for adapters that previously had zero
    get_columns_in_relation calls.

    Run with: pytest <path> -s -k benchmark
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {f"wide_{n}.sql": _make_wide_model(n) for n in _COLUMN_COUNTS}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_benchmark_get_columns_in_relation(self, project):
        results = {}
        with project.adapter.connection_named("_benchmark"):
            for n in _COLUMN_COUNTS:
                relation = project.adapter.Relation.create(
                    database=project.database,
                    schema=project.test_schema,
                    identifier=f"wide_{n}",
                )

                # warmup
                project.adapter.get_columns_in_relation(relation)

                times = []
                for _ in range(_ITERATIONS):
                    start = time.perf_counter()
                    cols = project.adapter.get_columns_in_relation(relation)
                    elapsed_ms = (time.perf_counter() - start) * 1000
                    times.append(elapsed_ms)

                assert len(cols) == n, f"Expected {n} columns, got {len(cols)}"

                results[n] = {
                    "avg_ms": sum(times) / len(times),
                    "min_ms": min(times),
                    "max_ms": max(times),
                }

        lines = []
        lines.append("")
        lines.append("=== get_columns_in_relation Benchmark ===")
        lines.append(f"  Adapter: {project.adapter.type()}")
        lines.append(f"  Iterations per size: {_ITERATIONS}")
        lines.append("")
        lines.append(f"{'Columns':>10} {'Avg (ms)':>10} {'Min (ms)':>10} {'Max (ms)':>10}")
        lines.append("-" * 44)
        for n in _COLUMN_COUNTS:
            r = results[n]
            lines.append(f"{n:>10} {r['avg_ms']:>10.2f} {r['min_ms']:>10.2f} {r['max_ms']:>10.2f}")
        lines.append("=" * 44)

        pytest.fail("\n".join(lines), pytrace=False)
