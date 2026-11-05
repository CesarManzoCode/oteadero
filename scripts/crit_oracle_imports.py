"""Criterion D23: the oracle imports only the standard library and its own modules."""

import argparse
import ast
import sys
from pathlib import Path

FORBIDDEN_MENTIONS = ("internal/", "cmd/", "harness/", "oteadero")


def check_file(path, oracle_dir, local_modules):
    """Return the findings of one Python file as printable lines."""
    relative = path.relative_to(oracle_dir)
    try:
        tree = ast.parse(path.read_bytes(), filename=str(path))
    except (SyntaxError, ValueError) as err:
        return [f"{path}: cannot parse: {err}"]

    findings = []
    allowed = sys.stdlib_module_names | local_modules
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            modules = [node.module]
        elif isinstance(node, ast.ImportFrom):
            modules = []
            if node.level > len(relative.parts):
                findings.append(f"{path}:{node.lineno}: relative import leaves the oracle")
        elif isinstance(node, ast.Constant) and isinstance(node.value, (str, bytes)):
            modules = []
            text = node.value
            if isinstance(text, bytes):
                text = text.decode("latin-1")
            for mention in FORBIDDEN_MENTIONS:
                if mention in text:
                    findings.append(f"{path}:{node.lineno}: string mentions {mention!r}")
        else:
            continue
        for module in modules:
            if module.split(".")[0] not in allowed:
                findings.append(f"{path}:{node.lineno}: forbidden import {module!r}")
    return findings


def check_tree(oracle_dir):
    """Return the findings of every Python file under oracle_dir."""
    files = sorted(oracle_dir.rglob("*.py"))
    if not files:
        return [f"{oracle_dir}: no Python files to inspect"]
    local_modules = {p.relative_to(oracle_dir).parts[0].removesuffix(".py") for p in files}
    findings = []
    for path in files:
        findings.extend(check_file(path, oracle_dir, local_modules))
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="tree holding oracle/, or the oracle directory itself",
    )
    args = parser.parse_args()
    nested = args.root / "oracle"
    findings = check_tree(nested if nested.is_dir() else args.root)
    if findings:
        for finding in findings:
            print(finding)
        return 1
    print("oracle imports: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
