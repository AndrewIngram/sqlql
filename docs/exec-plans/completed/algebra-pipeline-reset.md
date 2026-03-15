# Algebra Pipeline Reset

Status: completed on this branch.

## Major adopted decisions

- SQL lowers into canonical logical relational algebra.
- Canonical logical planning is the semantic source of truth.
- Rewrite stages produce a rewritten rel before execution.
- Physical planning assigns `local` or `provider:<name>` ownership to canonical rel subtrees.
- Whole-query pushdown is the root-fragment case of the same model.
- Recursive CTEs, derived tables, correlated decorrelatable forms, and richer windows now flow through the rel-first pipeline.

## Architectural outcomes

- SQL rel fallback was removed from the canonical IR.
- `correlate`, `cte_ref`, `repeat_union`, and `values` are first-class rel nodes.
- Local execution is the semantic baseline.
- Executor consumes rewritten rel rather than redoing planner rewrite work.

## Remaining follow-up

- `explain()` provider compilation behavior is still a live architectural question.
- Fragment planning is still greedy rather than bottom-up support analysis or cost-based search.
