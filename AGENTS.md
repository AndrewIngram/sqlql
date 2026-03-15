# AGENTS.md

Use this file as the repo entrypoint, not the full knowledge base.

Canonical docs:

- [Architecture map](./ARCHITECTURE.md)
- [Docs index](./docs/index.md)
- [Design docs index](./docs/design-docs/index.md)
- [Execution plans](./docs/exec-plans/active/README.md)

## Verification

For any non-trivial code change, always run:

- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`
- `pnpm fmt`

Do this before finalizing the work unless the user explicitly asks not to.

Notes:

- `pnpm typecheck` is the canonical workspace typecheck and must cover all packages (`pnpm -r typecheck`).
- Use `pnpm typecheck:root` only when you explicitly want the root tsconfig check by itself.
- Use `pnpm verify:ci` when you need local CI parity.

## Current-State Policy

- Optimize for one canonical current-state implementation, not compatibility with historical local states.
- Do not preserve or introduce compatibility bridges, migration shims, silent fallbacks, or dual behavior unless the user explicitly asks for that support.
- Prefer one explicit codepath with fail-fast diagnostics.

The deeper design rationale lives in [core beliefs](./docs/design-docs/core-beliefs.md) and [planner invariants](./docs/design-docs/planner-invariants.md).

## Package Boundaries

Keep the canonical package layering acyclic:

- `@tupl/foundation`
- `@tupl/provider-kit`
- `@tupl/schema-model`
- `@tupl/planner`
- `@tupl/runtime`
- `@tupl/schema`

Within those six packages, only import downward along that layering.

See [package architecture](./docs/package-architecture.md) for the detailed contract.

## Workflow Rules

- Substantial architectural work should start with a checked-in active execution plan under [`docs/exec-plans/active`](./docs/exec-plans/active/README.md).
- Completed architectural work should update the relevant durable design docs in the same branch and move the plan to [`docs/exec-plans/completed`](./docs/exec-plans/completed).
- Mechanical enforcement for “plan coverage by diff” is intentionally deferred for now and tracked in [tech debt](./docs/exec-plans/tech-debt-tracker.md).
- Be suspicious of casts. They are often a sign that a boundary or helper is failing to preserve the right type information.
- Keep casts only when they are clearly narrowing something the type system cannot express directly; otherwise prefer fixing the owning type surface.

## Repo-Local Skills

- [`better-result`](./.agents/skills/better-result/SKILL.md): migrate expected library failures to typed `Result` flows.
- [`software-design-philosophy`](./.agents/skills/software-design-philosophy/SKILL.md): keep modules deep, hide complexity, avoid shallow interfaces.
- [`typescript`](./.agents/skills/typescript/SKILL.md): prefer inference for internal code; keep explicit return types only when they carry API intent.
