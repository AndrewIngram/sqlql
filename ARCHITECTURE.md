# Architecture Map

This file is the stable map to the current `tupl` architecture and repo knowledge system.

Current design status:

- rel-first planner/runtime pipeline is the canonical path
- providers compile canonical `RelNode` subtrees
- local runtime is the semantic baseline
- execution plans and durable design docs live in-repo

Start here:

- [Docs index](./docs/index.md)
- [Core beliefs](./docs/design-docs/core-beliefs.md)
- [Relational pipeline](./docs/design-docs/relational-pipeline.md)
- [Provider model](./docs/design-docs/provider-model.md)
- [Planner invariants](./docs/design-docs/planner-invariants.md)
- [Package architecture](./docs/package-architecture.md)
- [SQL standards roadmap](./docs/sql-standards-roadmap.md)
- [Tech debt tracker](./docs/exec-plans/tech-debt-tracker.md)

Repository operating rules:

- [AGENTS.md](./AGENTS.md) contains the short, repo-wide execution rules.
- Substantial architectural work should be represented by a checked-in execution plan under [`docs/exec-plans`](./docs/exec-plans/active/README.md).
- Completed architectural work should update both the completed plan archive and the relevant durable design docs.
