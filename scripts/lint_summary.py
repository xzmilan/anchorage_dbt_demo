#!/usr/bin/env python3
"""
Parse sqlfluff-output.json and print a per-rule violation summary table.
Used in CI to surface lint failures clearly in GitHub Actions logs.

Usage:
    python3 scripts/lint_summary.py [--input sqlfluff-output.json] [--fail-on-violations]
"""

import json
import sys
import argparse
from pathlib import Path
from collections import defaultdict


RULE_DESCRIPTIONS = {
    "AL01": "Implicit/explicit aliasing of table",
    "AL02": "Implicit/explicit aliasing of columns",
    "AL03": "Column expression without alias",
    "AL04": "Table alias reuse",
    "AL05": "Tables aliased but not referenced",
    "AL06": "Implicit/explicit aliasing of columns",
    "AM01": "Avoid DISTINCT in aggregate",
    "AM02": "Short-circuit evaluation",
    "AM03": "Ambiguous ORDER BY in window",
    "AM04": "SELECT * in queries",
    "AM05": "Join condition without equalities",
    "AM06": "Ambiguous column reference",
    "AM07": "Ambiguous column reference in USING",
    "CP01": "Inconsistent capitalisation of keywords",
    "CP02": "Inconsistent capitalisation of identifiers",
    "CP03": "Inconsistent capitalisation of function names",
    "CP04": "Inconsistent capitalisation of boolean/null literal",
    "CP05": "Inconsistent capitalisation of datatypes",
    "CV01": "Use of DISTINCT",
    "CV02": "Use of ISNULL or IFNULL",
    "CV03": "Use NVL or IFNULL",
    "CV04": "Use of COALESCE",
    "CV05": "Use of NULL comparison operators",
    "CV06": "Statements must end with a semi-colon",
    "CV07": "Top-level statements must not be wrapped",
    "CV08": "LEFT JOIN instead of RIGHT JOIN",
    "CV09": "Use of CONVERT",
    "CV10": "Use of blind EXCEPT",
    "CV11": "Enforce a consistent syntax for WITHIN GROUP",
    "JJ01": "Jinja delimiters",
    "LT01": "Unnecessary trailing whitespace",
    "LT02": "Incorrect indentation",
    "LT03": "Operators should be surrounded by single spaces",
    "LT04": "Commas should be at the end of the line",
    "LT05": "Long lines [> 120 chars]",
    "LT06": "Function name not immediately followed by parenthesis",
    "LT07": "Statement not immediately followed by newline",
    "LT08": "Blank line expected but not found",
    "LT09": "Select wildcards then simple column refs",
    "LT10": "SELECT modifiers (DISTINCT etc.) on the same line",
    "LT11": "Set operators surrounded by newlines",
    "LT12": "Files must end with a trailing newline",
    "LT13": "Files must not begin with newlines or whitespace",
    "RF01": "References cannot reference objects not in FROM clause",
    "RF02": "References should be qualified",
    "RF03": "References should be consistent",
    "RF04": "Keywords should not be used as identifiers",
    "RF05": "Use back-ticks for column names",
    "RF06": "Unnecessary quoted identifier",
    "ST01": "Do not use ELSE NULL in a case when statement",
    "ST02": "Unnecessary CASE statement",
    "ST03": "Unused variables in Query",
    "ST04": "Nested CASE statement in ELSE clause",
    "ST05": "Join/From clauses should not contain subqueries",
    "ST06": "Select wildcards then simple column refs before calculations",
    "ST07": "Prefer USING over ON for simple joins",
    "ST08": "Prefer BOOLEAN expression",
    "ST09": "Prefer UNION ALL over UNION",
    "TQ01": "Unnecessary nested WITH",
}


def parse_violations(data):
    """Extract rule violations from sqlfluff JSON output."""
    violations_by_rule = defaultdict(int)
    total_files = 0
    files_with_violations = 0

    files = data if isinstance(data, list) else data.get("files", [])

    for file_result in files:
        total_files += 1
        file_violations = file_result.get("violations", [])
        if file_violations:
            files_with_violations += 1
        for v in file_violations:
            rule_code = v.get("code", "UNKNOWN")
            violations_by_rule[rule_code] += 1

    return violations_by_rule, total_files, files_with_violations


def print_summary(violations_by_rule, total_files, files_with_violations):
    """Print formatted violation summary table."""
    total_violations = sum(violations_by_rule.values())

    print()
    print("=" * 72)
    print("  SQLFluff Lint Summary")
    print("=" * 72)
    print(f"  Files checked   : {total_files}")
    print(f"  Files with issues: {files_with_violations}")
    print(f"  Total violations: {total_violations}")
    print("=" * 72)

    if violations_by_rule:
        print(f"  {'Rule':<8}  {'Count':>6}  Description")
        print(f"  {'-'*8}  {'-'*6}  {'-'*48}")
        for rule, count in sorted(violations_by_rule.items()):
            desc = RULE_DESCRIPTIONS.get(rule, "See SQLFluff docs")
            print(f"  {rule:<8}  {count:>6}  {desc}")
        print(f"  {'-'*8}  {'-'*6}  {'-'*48}")
        print(f"  {'TOTAL':<8}  {total_violations:>6}")
    else:
        print("  No violations found.")

    print("=" * 72)
    print()

    return total_violations


def main():
    parser = argparse.ArgumentParser(description="Summarize SQLFluff JSON output")
    parser.add_argument(
        "--input",
        default="sqlfluff-output.json",
        help="Path to sqlfluff JSON output file (default: sqlfluff-output.json)",
    )
    parser.add_argument(
        "--fail-on-violations",
        action="store_true",
        help="Exit with code 1 if any violations found",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(2)

    try:
        with open(input_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse {input_path}: {e}", file=sys.stderr)
        sys.exit(2)

    violations_by_rule, total_files, files_with_violations = parse_violations(data)
    total = print_summary(violations_by_rule, total_files, files_with_violations)

    if args.fail_on_violations and total > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
