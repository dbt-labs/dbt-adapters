import argparse
import ast
from pathlib import Path
from typing import Iterator, List


Import = List[str]


def get_imports(module: Path) -> Iterator[Import]:
    with open(module) as fh:
        parsed_module = ast.parse(fh.read(), module)

    for node in ast.iter_child_nodes(parsed_module):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imported_module_path = getattr(node, "module", "")
            for imported_object_path in node.names:
                yield imported_module_path.split(".") + imported_object_path.name.split(".")


def invalid_import(module: Import) -> bool:
    return (
        len(module) > 1 and
        module[0] == "dbt" and
        module[1] not in ["adapters", "include"]
    )


def check_package(package: Path):
    for module in package.rglob("*.py"):
        for imported_module in get_imports(module):
            if invalid_import(imported_module):
                offending_module = module.relative_to(package)
                imported_module_path = ".".join(imported_module)
                raise Exception(f"A dbt-core module is being imported in {offending_module}: {imported_module_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("package", type=str)
    args = parser.parse_args()
    check_package(Path(args.package))


if __name__ == "__main__":
    main()
