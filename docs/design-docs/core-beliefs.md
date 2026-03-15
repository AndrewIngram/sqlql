# Core Beliefs

These are the stable engineering beliefs that should guide work across packages.

## Current-State Bias

- Prefer one canonical current-state implementation.
- Delete compatibility bridges rather than carrying them forward.
- Fail fast with specific diagnostics instead of adding silent fallback behavior.

## Semantic Ownership

- Relational algebra is the semantic source of truth.
- Providers are optimization backends over canonical rel subtrees, not alternate semantic APIs.
- The local runtime is the semantic baseline for supported query behavior.

## Provider Model

- `canExecute(rel)` is the provider truth source.
- Provider helper vocabulary belongs inside authoring helpers, not as metadata-first public contract.
- Narrow providers should still look relational at the boundary.

## Error Model

- Use `better-result` for expected library failures.
- Reserve thrown errors for genuine programmer defects or broken invariants.

## Type Surface

- Prefer inference for internal functions and locals.
- Add explicit return types only when they express public API intent or solve a real correctness problem.

## Complexity Management

- Favor deep modules that own hard concepts.
- Keep public surfaces small.
- Move durable decisions out of chat history and PR history into versioned docs and plans.
