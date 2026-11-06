"""Criterion: the harness has fewer lines than the engine (D26).

Lines count when they are not blank and hold something besides comments: Go
without // and /* */, Python without # and without docstrings. The engine is
cmd/ and internal/ without _test.go; the harness is harness/, oracle/ and
scripts/ without _test.go nor test_*.py, with the mutant patches, whose every
non-blank line counts.
"""

import ast
import io
import sys
import tokenize
from pathlib import Path

import lib

ENGINE_DIRS = ("cmd", "internal")
HARNESS_DIRS = ("harness", "oracle", "scripts")

_NOT_CODE = {
    tokenize.COMMENT,
    tokenize.NL,
    tokenize.NEWLINE,
    tokenize.INDENT,
    tokenize.DEDENT,
    tokenize.ENCODING,
    tokenize.ENDMARKER,
}


def go_lines(text):
    """Return the lines of Go source with something outside comments.

    Strings, runes and raw strings are code, so // or /* inside them do not
    start a comment.
    """
    code = set()
    line = 1
    state = None  # None, "line", "block" or the quote of the open literal
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
            if state == "line":
                state = None
            i += 1
            continue
        if state == "line":
            i += 1
        elif state == "block":
            if text.startswith("*/", i):
                state = None
                i += 2
            else:
                i += 1
        elif state is not None:
            if not c.isspace():
                code.add(line)
            if c == "\\" and state != "`" and i + 1 < n and text[i + 1] != "\n":
                i += 2
                continue
            if c == state:
                state = None
            i += 1
        elif text.startswith("//", i):
            state = "line"
            i += 2
        elif text.startswith("/*", i):
            state = "block"
            i += 2
        else:
            if not c.isspace():
                code.add(line)
            if c in "\"'`":
                state = c
            i += 1
    return len(code)


def _docstring_starts(tree):
    """Return the (line, column) where each docstring of a module starts."""
    starts = set()
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            continue
        if node.body and isinstance(node.body[0], ast.Expr):
            value = node.body[0].value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                starts.add((value.lineno, value.col_offset))
    return starts


def python_lines(text):
    """Return the non-blank lines of Python source with a token besides comments and docstrings."""
    docstrings = _docstring_starts(ast.parse(text))
    rows = set()
    for tok in tokenize.generate_tokens(io.StringIO(text).readline):
        if tok.type in _NOT_CODE:
            continue
        if tok.type == tokenize.STRING and tok.start in docstrings:
            continue
        rows.update(range(tok.start[0], tok.end[0] + 1))
    lines = text.splitlines()
    return sum(1 for row in rows if row <= len(lines) and lines[row - 1].strip())


def patch_lines(text):
    return sum(1 for line in text.splitlines() if line.strip())


def file_lines(path):
    """Return the counted lines of a file, or None if it is a test or not counted."""
    name = path.name
    if name.endswith("_test.go") or (name.startswith("test_") and name.endswith(".py")):
        return None
    counter = {".go": go_lines, ".py": python_lines, ".patch": patch_lines}.get(path.suffix)
    if counter is None:
        return None
    return counter(path.read_text(encoding="utf-8"))


def tree_lines(root, dirs):
    total = 0
    for d in dirs:
        for path in sorted((Path(root) / d).rglob("*")):
            if path.is_file():
                total += file_lines(path) or 0
    return total


def main():
    engine = tree_lines(lib.REPO_ROOT, ENGINE_DIRS)
    harness = tree_lines(lib.REPO_ROOT, HARNESS_DIRS)
    print(f"motor: {engine}")
    print(f"arnés: {harness}")
    if engine == 0:
        print("cociente arnés/motor: indefinido (motor sin líneas)")
        return 1
    print(f"cociente arnés/motor: {harness / engine:.3f}")
    return 0 if harness < engine else 1


if __name__ == "__main__":
    sys.exit(main())
