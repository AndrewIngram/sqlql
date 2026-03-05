# Triage Playbook

Use this playbook for the first pass of a security audit.

## 1) Map Trust Boundaries

Identify:
- public inputs (HTTP requests, query strings, SQL text, headers, body, events)
- trust pivots (auth/session context builders, policy loaders, tenant scope builders)
- sinks (database operations, response serialization, file/network side effects)

Output:
- boundary map with entrypoint -> trust pivot -> sink links

## 2) Inventory Public Interface Surface

Enumerate interface points that can be influenced by untrusted callers.

For library/runtime code, prioritize:
- parser inputs
- planner/optimizer inputs
- schema/table/column exposure controls
- provider/adapter hooks
- fallback/local-execution paths

Output:
- attack-surface table with file paths and owner components

## 3) Build Candidate Abuse Paths

For each entrypoint, attempt abuse chains:
- widen data shape
- bypass auth scoping
- force expensive execution path
- inject expression fragments

Reject candidates that lack a feasible path from untrusted input.

Output:
- shortlist of suspicious paths for deep dive

## 4) Stop/Go Rules

Go to deep dive when at least one condition is true:
- evidence of missing authorization at boundary construction
- ability to reference undeclared/sensitive data fields
- exploitable injection path to query/evaluation sink
- attacker-controlled path to resource exhaustion with weak guardrails

Stop after triage if none are true and report residual risks.
