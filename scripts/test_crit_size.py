import tempfile
import unittest
from pathlib import Path

import crit_size

GO_SOURCE = """// Package x has a header comment.
package x

/* a block
   comment */
import "fmt" // trailing

func f() string {
\ts := "// not a comment"
\tq := '\\''
\tr := `raw
/* still raw */

end`
\t/* inline */ return s + r + string(q)
\t/* only a comment */
}
"""
# package, import, func, s, q, r, still raw, end, return, }
GO_LINES = 10

PY_SOURCE = '''"""Module docstring
spanning lines."""

# comment
import os  # trailing


class C:
    """Class doc."""

    def f(self):
        """Function doc.

        More.
        """
        s = """not a
docstring

end"""
        return s  # c


def g(): """one line"""
'''
# import, class, def f, the three non-blank lines of s, return, def g
PY_LINES = 8

PATCH = """diff --git a/x.go b/x.go
--- a/x.go
+++ b/x.go

@@ -1 +1 @@
-a
+b
"""
PATCH_LINES = 6


class CounterTest(unittest.TestCase):
    def test_go(self):
        self.assertEqual(crit_size.go_lines(GO_SOURCE), GO_LINES)

    def test_python(self):
        self.assertEqual(crit_size.python_lines(PY_SOURCE), PY_LINES)

    def test_patch(self):
        self.assertEqual(crit_size.patch_lines(PATCH), PATCH_LINES)


class TreeTest(unittest.TestCase):
    def test_engine_and_harness_without_tests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            files = {
                "cmd/a/main.go": GO_SOURCE,
                "cmd/a/main_test.go": GO_SOURCE,
                "internal/b/x.go": GO_SOURCE,
                "internal/b/notes.txt": "not counted\n",
                "harness/c/y.go": GO_SOURCE,
                "harness/c/y_test.go": GO_SOURCE,
                "harness/mutants/M1.patch": PATCH,
                "oracle/o.py": PY_SOURCE,
                "oracle/test_o.py": PY_SOURCE,
                "scripts/s.py": PY_SOURCE,
                "scripts/test_s.py": PY_SOURCE,
            }
            for name, text in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            self.assertEqual(crit_size.tree_lines(root, crit_size.ENGINE_DIRS), 2 * GO_LINES)
            self.assertEqual(
                crit_size.tree_lines(root, crit_size.HARNESS_DIRS),
                GO_LINES + PATCH_LINES + 2 * PY_LINES,
            )


if __name__ == "__main__":
    unittest.main()
