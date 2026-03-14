# Planner Invariants

These invariants should hold unless a deliberate architecture change updates this file.

## Logical model

- There is no SQL rel fallback node in the canonical logical IR.
- Supported queries lower to canonical rel or fail with tagged planning errors.
- Final executable plans should not contain unresolved scalar/EXISTS subquery expressions.

## Provider ownership

- `cte_ref` is a local barrier for provider ownership.
- `values`, `correlate`, and `repeat_union` are also local ownership barriers under the current
  execution model.
- Cross-provider or local-only nodes must not be misattributed to one provider just because nearby scans share a provider.
- Providers receive normalized rel subtrees, not planner-private wrapper nodes.

## Rewrite and execution

- The executor consumes rewritten rel, not raw lowered rel.
- Planner rewrite work does not happen again inside the executor.
- View expansion and decorrelation belong to planner rewrite stages.
- `explain()` has explicit basic and enriched provider-description modes; only enriched mode may
  compile provider fragments.

## Window semantics

- `ROWS` frames are supported.
- Non-`ROWS` frame modes are explicitly unsupported until semantics are implemented correctly.

## Error and type surfaces

- Expected library failures should use tagged `Result` errors.
- Internal helper signatures should prefer inference over wide explicit return annotations.
