import json
from pathlib import Path
import agate


def save_agate_table(table, base_path):
    """Save an agate table to disk with type preservation.

    Args:
        table: An agate.Table object
        base_path: Base path/filename without extension (e.g., 'data/my_table')
    """
    # Create directory if it doesn't exist
    Path(base_path).parent.mkdir(parents=True, exist_ok=True)

    # Convert table data to list of dictionaries
    data = [dict(zip(table.column_names, row)) for row in table.rows]

    # Save the data as JSON
    with open(f"{base_path}.json", "w") as f:
        json.dump(data, f, indent=2, default=str)

    # Save the column types metadata
    column_types = {}
    for name, column in table.columns.items():
        if isinstance(column.data_type, agate.Number):
            # Check if column has decimals
            decimals = table.aggregate(agate.MaxPrecision(table.column_names.index(name)))
            column_types[name] = "Double" if decimals else "Integer"
        else:
            column_types[name] = type(column.data_type).__name__

    with open(f"{base_path}_metadata.json", "w") as f:
        json.dump(column_types, f, indent=2)


def load_agate_table(base_path):
    """Load an agate table from disk with preserved types.

    Args:
        base_path: Base path/filename without extension (e.g., 'data/my_table')

    Returns:
        An agate.Table object
    """
    # Load the column types metadata
    with open(f"{base_path}_metadata.json", "r") as f:
        column_types = json.load(f)

    # Map string names to actual agate types
    type_mapping = {
        "Text": agate.Text(),
        "Double": agate.Number(),
        "Integer": agate.Number(),
        "Boolean": agate.Boolean(),
        "Date": agate.Date(),
        "DateTime": agate.DateTime(),
        "TimeDelta": agate.TimeDelta(),
    }

    # Load the data
    with open(f"{base_path}.json", "r") as f:
        data = json.load(f)

    if not data:
        return agate.Table([], [])

    # Get column names from the first row
    column_names = list(data[0].keys())

    # Create the column types list in the correct order
    column_types_list = [type_mapping[column_types[name]] for name in column_names]

    # Convert data to rows
    rows = [[row[col] for col in column_names] for row in data]

    # Create and return the table
    return agate.Table(rows, column_names, column_types_list)
