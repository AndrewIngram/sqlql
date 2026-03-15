# Translation Introspection and Provider Surface Reset

Status: completed on this branch.

## Major adopted decisions

- `explain()` exposes staged translation artifacts rather than a single opaque rel.
- Planner tests can assert normalized whole-tree shapes.
- Provider contract is rel-first:
  - `canExecute(rel)`
  - `compile(rel)`
  - optional `describeCompiledPlan(plan)`
  - `execute(plan)`
- Capability metadata moved out of the primary provider contract and into helper-level analysis.

## Architectural outcomes

- Providers compile canonical rel subtrees instead of scan/aggregate fragment families.
- Helper-level shape analysis now exists for narrow providers, including simple scans and keyed scans.
- Redis follows the rel-first model while still supporting optional keyed optimization helpers.

## Remaining follow-up

- Decide whether helper-level capability atoms remain worthwhile or should be removed entirely.
- Deepen bottom-up provider support analysis if greedy probing becomes a practical limit.
