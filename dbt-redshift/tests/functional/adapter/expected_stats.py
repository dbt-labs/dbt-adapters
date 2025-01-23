from dbt.tests.util import AnyStringWith, AnyInteger, AnyString, AnyFloat


def redshift_stats():
    return {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": True,
            "description": "Indicates whether there are statistics for this table",
            "include": False,
        },
        "encoded": {
            "id": "encoded",
            "label": "Encoded",
            "value": AnyStringWith("Y"),
            "description": "Indicates whether any column in the table has compression encoding defined.",
            "include": True,
        },
        "diststyle": {
            "id": "diststyle",
            "label": "Dist Style",
            "value": AnyStringWith("AUTO"),
            "description": "Distribution style or distribution key column, if key distribution is defined.",
            "include": True,
        },
        "max_varchar": {
            "id": "max_varchar",
            "label": "Max Varchar",
            "value": AnyInteger(),
            "description": "Size of the largest column that uses a VARCHAR data type.",
            "include": True,
        },
        "size": {
            "id": "size",
            "label": "Approximate Size",
            "value": AnyInteger(),
            "description": "Approximate size of the table, calculated from a count of 1MB blocks",
            "include": True,
        },
        "sortkey1": {
            "id": "sortkey1",
            "label": "Sort Key 1",
            "value": AnyString(),
            "description": "First column in the sort key.",
            "include": True,
        },
        "pct_used": {
            "id": "pct_used",
            "label": "Disk Utilization",
            "value": AnyFloat(),
            "description": "Percent of available space that is used by the table.",
            "include": True,
        },
        "stats_off": {
            "id": "stats_off",
            "label": "Stats Off",
            "value": AnyFloat(),
            "description": "Number that indicates how stale the table statistics are; 0 is current, 100 is out of date.",
            "include": True,
        },
        "rows": {
            "id": "rows",
            "label": "Approximate Row Count",
            "value": AnyFloat(),
            "description": "Approximate number of rows in the table. This value includes rows marked for deletion, but not yet vacuumed.",
            "include": True,
        },
    }


def redshift_ephemeral_summary_stats():
    additional = {
        "skew_sortkey1": {
            "description": "Ratio of the size of the largest non-sort "
            "key column to the size of the first column "
            "of the sort key.",
            "id": "skew_sortkey1",
            "include": True,
            "label": "Sort Key Skew",
            "value": 1.0,
        },
        "sortkey_num": {
            "description": "Number of columns defined as sort keys.",
            "id": "sortkey_num",
            "include": True,
            "label": "# Sort Keys",
            "value": 1.0,
        },
        "unsorted": {
            "description": "Percent of unsorted rows in the table.",
            "id": "unsorted",
            "include": True,
            "label": "Unsorted %",
            "value": 0.0,
        },
    }
    stats = redshift_stats()
    stats.update(additional)
    return stats
