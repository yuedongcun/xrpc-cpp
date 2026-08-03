#!/usr/bin/env python3

import argparse
import codecs
import difflib
import fnmatch
import os
import subprocess
import sys


def is_source_file(filename):
    return filename.endswith(".h") or filename.endswith(".hpp") or filename.endswith(".cpp")


def should_exclude(filename, exclude_globs):
    return any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_globs)


def collect_source_files(source_dir, exclude_globs):
    source_files = []

    for directory, _, filenames in os.walk(source_dir):
        for filename in filenames:
            fullpath = os.path.join(directory, filename)
            if is_source_file(fullpath) and not should_exclude(fullpath, exclude_globs):
                source_files.append(fullpath)

    return source_files


def check(args, source_dir, exclude_globs):
    source_files = collect_source_files(source_dir, exclude_globs)

    if not source_files:
        return False

    if args.fix:
        if not args.quiet:
            for filename in source_files:
                print(f"Formatting {filename}")
        subprocess.check_call([args.clang_format_binary, "-i"] + source_files)
        return False

    has_error = False

    for filename in source_files:
        if not args.quiet:
            print(f"Checking {filename}")

        with open(filename, "rb") as reader:
            formatted = subprocess.check_output([args.clang_format_binary, filename])
            formatted = codecs.decode(formatted, "utf-8")
            original = codecs.decode(reader.read(), "utf-8")

        diff = list(
            difflib.unified_diff(
                original.splitlines(True),
                formatted.splitlines(True),
                fromfile=filename,
                tofile=f"{filename} (after clang-format)",
            )
        )

        if diff:
            print(f"{filename} had clang-format style issues")
            sys.stderr.writelines(diff)
            has_error = True

    return has_error


def main():
    parser = argparse.ArgumentParser(description="Run clang-format on source files.")
    parser.add_argument("clang_format_binary", help="Path to clang-format")
    parser.add_argument("exclude_globs", help="File containing exclude glob patterns")
    parser.add_argument(
        "--source_dirs", required=True, help="Comma-separated source directories"
    )
    parser.add_argument("--fix", action="store_true", help="Format files in place")
    parser.add_argument("--quiet", action="store_true", help="Only print errors")
    args = parser.parse_args()

    with open(args.exclude_globs, encoding="utf-8") as exclude_file:
        exclude_globs = [
            line.strip()
            for line in exclude_file
            if line.strip() and not line.startswith("#")
        ]

    has_error = False

    for source_dir in args.source_dirs.split(","):
        if source_dir:
            has_error = check(args, source_dir, exclude_globs) or has_error

    return 1 if has_error else 0


if __name__ == "__main__":
    sys.exit(main())
