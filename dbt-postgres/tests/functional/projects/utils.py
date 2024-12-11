from pathlib import Path


FILE_TYPES = {
    "data": "csv",
    "docs": "md",
    "macros": "sql",
    "models": "sql",
    "schemas": "yml",
    "staging": "sql",
}


def read(project: str, file_type: str, file_name: str) -> str:
    root = Path(__file__).parent / project
    extension = FILE_TYPES[file_type]
    file = root / file_type / f"{file_name}.{extension}"
    contents = file.read_text()
    if file_type == "data":
        return contents.strip()
    return contents
