# `@tupl/provider-kysely`

Kysely-backed provider builders for tupl.

Use this package to build a tupl provider over Kysely-backed tables.

## Install

```bash
pnpm add @tupl/provider-kysely kysely
```

## Docs

- Repository: <https://github.com/AndrewIngram/tupl>
- Provider contracts and adapter-authoring helpers: [`@tupl/provider-kit`](https://github.com/AndrewIngram/tupl/tree/main/packages/provider-kit)
- Schema APIs: [`@tupl/schema`](https://github.com/AndrewIngram/tupl/tree/main/packages/schema)

## Design Notes

- `@tupl/provider-kit` owns adapter plumbing and capability reporting.
- This package owns Kysely-specific planning and execution.
- [`src/index.ts`](./src/index.ts) should stay thin; backend logic belongs in `planning/`, `execution/`, and `backend/`.
