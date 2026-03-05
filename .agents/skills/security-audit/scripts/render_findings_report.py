#!/usr/bin/env python3
"""Render a deterministic findings-first Markdown report from JSON findings."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = [
    "severity",
    "title",
    "vulnerability",
    "exploit_path",
    "impact",
    "evidence",
    "remediation",
    "required_tests",
]

SEVERITY_ORDER = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render findings-first markdown report")
    parser.add_argument("--input", required=True, help="Path to findings JSON")
    parser.add_argument("--output", required=True, help="Path to output markdown")
    parser.add_argument("--scope", default="public-interface", help="Audit scope label")
    return parser.parse_args()


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_findings(path: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"input file not found: {path}")
    except json.JSONDecodeError as exc:
        fail(f"invalid JSON: {exc}")

    if not isinstance(data, list):
        fail("input must be a JSON array of findings")

    validated: list[dict[str, Any]] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            fail(f"finding at index {index} must be an object")

        missing = [field for field in REQUIRED_FIELDS if field not in item]
        if missing:
            fail(f"finding at index {index} missing required field(s): {', '.join(missing)}")

        severity = item["severity"]
        if severity not in SEVERITY_ORDER:
            fail(
                f"finding at index {index} has invalid severity '{severity}' "
                "(expected one of: critical, high, medium, low)"
            )

        validated.append(item)

    return validated


def format_list(value: Any) -> str:
    if isinstance(value, list):
        return "\n".join(f"- {str(item)}" for item in value)
    return f"- {str(value)}"


def render_report(findings: list[dict[str, Any]], scope: str) -> str:
    sorted_findings = sorted(
        findings,
        key=lambda finding: (
            SEVERITY_ORDER[finding["severity"]],
            str(finding["title"]).lower(),
        ),
    )

    lines: list[str] = [
        "# Security Audit Report",
        "",
        f"Scope: {scope}",
        "",
        "## Findings",
        "",
    ]

    if not sorted_findings:
        lines.extend(
            [
                "No confirmed findings.",
                "",
                "## Residual Risks and Testing Gaps",
                "- High-risk paths should still be covered by negative authorization and abuse-path tests.",
                "- Re-run audit after major query-planning, parser, or auth-boundary changes.",
                "",
            ]
        )
        return "\n".join(lines)

    for idx, finding in enumerate(sorted_findings, start=1):
        lines.extend(
            [
                f"### {idx}. [{finding['severity'].upper()}] {finding['title']}",
                f"- Vulnerability: {finding['vulnerability']}",
                f"- Exploit path: {finding['exploit_path']}",
                f"- Impact: {finding['impact']}",
                f"- Evidence: {finding['evidence']}",
                f"- Remediation: {finding['remediation']}",
                "- Required tests:",
                format_list(finding["required_tests"]),
                "",
            ]
        )

    lines.extend(
        [
            "## Open Questions and Assumptions",
            "- Document any unresolved trust-boundary assumptions made during analysis.",
            "",
            "## Change Summary",
            "- Summarize recommended fixes after triaging the findings above.",
            "",
        ]
    )

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    findings = load_findings(input_path)
    report = render_report(findings, args.scope)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
