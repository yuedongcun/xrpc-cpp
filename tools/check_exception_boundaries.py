#!/usr/bin/env python3

"""Reject unreviewed exception handlers in production code."""

from pathlib import Path
import re
import sys


BOUNDARY_MARKER = "XRPC_EXTERNAL_EXCEPTION_BOUNDARY"
GUARD_MARKER = "XRPC_EXCEPTION_GUARD"
CATCH_PATTERN = re.compile(r"\bcatch\s*\(", re.MULTILINE)
CATCH_ALL_PATTERN = re.compile(r"\bcatch\s*\(\s*\.\.\.\s*\)", re.MULTILINE)
SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"}
SOURCE_ROOTS = ("include", "src", "tests", "tools")


def source_files(repo_root: Path):
    for source_root in SOURCE_ROOTS:
        root = repo_root / source_root
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in SOURCE_SUFFIXES:
                yield path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    violations = []

    for path in source_files(repo_root):
        content = path.read_text(encoding="utf-8")
        pattern = CATCH_PATTERN if "include" in path.parts or "src" in path.parts else CATCH_ALL_PATTERN
        for match in pattern.finditer(content):
            line_number = content.count("\n", 0, match.start()) + 1
            line_end = content.find("\n", match.end())
            if line_end == -1:
                line_end = len(content)
            declaration_line = content[content.rfind("\n", 0, match.start()) + 1 : line_end]
            is_boundary = BOUNDARY_MARKER in declaration_line
            is_guard = GUARD_MARKER in declaration_line
            if not is_boundary and not is_guard:
                violations.append(f"{path.relative_to(repo_root)}:{line_number}")

    if not violations:
        return 0

    print("Unreviewed exception handlers are forbidden:", file=sys.stderr)
    for violation in violations:
        print(f"  {violation}", file=sys.stderr)
    print(
        f"Mark a true external boundary with {BOUNDARY_MARKER} or a required invariant guard with {GUARD_MARKER}.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
