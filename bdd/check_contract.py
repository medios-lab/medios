#!/usr/bin/env python3
"""Fast BDD-PR presence/shape gate. NOT an executable Gherkin validator.

Real step binding, independent observation, and PASS/FAIL evaluation belong to
the future Medios-backed acceptance harness; this script must not claim them.
"""
import os
import re
import pathlib
import subprocess
import sys


def changed_files(base, head):
    result = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMR", f"{base}...{head}"],
        check=True, text=True, capture_output=True,
    )
    return [p for p in result.stdout.splitlines() if p]


def is_runtime_path(path):
    return (path.startswith(("src/", "include/", "libprocess/", "stout/",
                             "tests/", "cmake/"))
            or path == "CMakeLists.txt")


def check_feature(path):
    content = pathlib.Path(path).read_text(encoding="utf-8")
    lines = [line.strip() for line in content.splitlines()
             if line.strip() and not line.lstrip().startswith("#")]
    if not any(line.startswith("Feature:") for line in lines):
        raise ValueError(f"{path}: missing Feature:")
    if not any(line.startswith(("Scenario:", "Scenario Outline:")) for line in lines):
        raise ValueError(f"{path}: missing Scenario:")
    if not any(line.startswith(("Given ", "When ", "Then ", "And ", "But ", "* ")) for line in lines):
        raise ValueError(f"{path}: missing scenario steps")


def main():
    base, head = sys.argv[1:3]
    changed = changed_files(base, head)
    features = [p for p in changed if p.startswith("bdd/features/") and p.endswith(".feature")]
    for path in features:
        check_feature(path)
    code_changed = any(is_runtime_path(p) for p in changed)
    if code_changed and not features:
        pr_body = os.environ.get("PR_BODY", "")
        # A maintainer must still confirm the exemption; this is presence only.
        exemptions = re.findall(r"(?m)^BDD-EXEMPT: +([^\\n]+)$", pr_body)
        if not exemptions or exemptions[0].strip().lower() in ("n/a", "none", "todo", "reason", "<reason>"):
            print("ERROR: behavioral/runtime changes require a bdd/features/**/*.feature "
                  "contract, or a reasoned BDD-EXEMPT section in the PR description.",
                  file=sys.stderr)
            return 1
        print("BDD-EXEMPT requested: requires maintainer confirmation.")
    print(f"BDD contract presence/shape check passed ({len(features)} changed features).")
    print("NOT_RUN: no Gherkin steps or independent oracle were executed by this check.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError, IndexError) as exc:
        print(f"BDD contract gate error: {exc}", file=sys.stderr)
        sys.exit(1)
