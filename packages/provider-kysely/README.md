# `@tupl/provider-kysely`

Kysely provider adapter for tupl.

Use this package to expose Kysely-backed tables as tupl providers.

## Install

```bash
pnpm add @tupl/provider-kysely kysely
```

## Docs

- Repository: <https://github.com/AndrewIngram/tupl>
- Adapter contracts: [`@tupl/provider-kit`](https://github.com/AndrewIngram/tupl/tree/main/packages/provider-kit)
- Schema APIs: [`@tupl/schema`](https://github.com/AndrewIngram/tupl/tree/main/packages/schema)

## Design Notes

- `@tupl/provider-kit` owns adapter plumbing and capability reporting.
- This package owns Kysely-specific planning and execution.
- [`src/index.ts`](./src/index.ts) should stay thin; backend logic belongs in `planning/`, `execution/`, and `backend/`.
