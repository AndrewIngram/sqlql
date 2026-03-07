---
name: security-audit
description: Findings-first security auditing for TypeScript/Node libraries and runtimes exposed to untrusted or public interfaces. Use when after any significant refactor, or when users ask for a security audit, auth-boundary review, schema/public-interface risk review, data-exposure analysis, or query-abuse/DoS analysis
---

# Security Audit

Run a structured, findings-first security audit for TypeScript/Node codebases, with emphasis on public interface abuse paths and authorization boundaries.

## Workflow

### Phase 1: Triage (fast, high-signal)
1. Map trust boundaries.
2. Enumerate untrusted entrypoints and externally reachable interfaces.
3. Trace sensitive data sources and exposure sinks.
4. Identify candidate abuse paths.

Stop triage and escalate to deep dive when at least one suspicious path has plausible exploitability.

### Phase 2: Deep Dive (only suspicious paths)
1. Validate exploitability with code-level evidence.
2. Confirm impact and blast radius.
3. Distinguish true vulnerabilities from hardening/performance concerns.
4. Define concrete remediations and required regression tests.

## Required Output Contract

Report findings first, sorted by severity (`critical`, `high`, `medium`, `low`).

Each finding must include:
- `severity`
- `title`
- `vulnerability`
- `exploit_path`
- `impact`
- `evidence` (file/line references)
- `remediation`
- `required_tests`

After findings, include:
- open questions/assumptions
- short change summary

If no findings are confirmed, explicitly state that and include residual risks/testing gaps.

## TypeScript/Node Focus Checks

Prioritize these classes:
- Query/SQL injection from string interpolation or unsafe expression composition
- Schema facade or projection bypass leading to undeclared field exposure
- Missing authorization checks at boundary construction points
- Unsafe dynamic evaluation/deserialization (`eval`, `Function`, permissive parsing)
- Unbounded query/resource shapes causing availability or cost abuse

Use targeted source search before drawing conclusions. Favor concrete exploit chains over speculative warnings.

## Severity Guidance

- `critical`: direct unauthorized data access or remote code execution with low attacker effort
- `high`: strong exploit path to sensitive data or control-plane actions
- `medium`: real weakness requiring preconditions or limited scope
- `low`: hardening gap with low immediate exploitability

## References

- `references/triage-playbook.md` for sequencing and stop/go rules.
- `references/finding-catalog-ts-node.md` for TS/Node vulnerability patterns and grep heuristics.
- `references/security-test-matrix.md` for required regression tests by finding class.
- `scripts/render_findings_report.py` for deterministic findings-first report generation from JSON findings.
