# `@tupl/provider-drizzle`

Drizzle adapter helpers for tupl.

Use this package to expose Drizzle-backed entities and tables as tupl providers.

## Install

```bash
pnpm add @tupl/provider-drizzle drizzle-orm
```

## Docs

- Repository: <https://github.com/AndrewIngram/tupl>
- Adapter contracts: [`@tupl/provider-kit`](https://github.com/AndrewIngram/tupl/tree/main/packages/provider-kit)
- Schema APIs: [`@tupl/schema`](https://github.com/AndrewIngram/tupl/tree/main/packages/schema)

## Design Notes

- `@tupl/provider-kit` owns adapter plumbing and capability reporting.
- This package owns Drizzle-specific planning and execution.
- [`src/index.ts`](./src/index.ts) should stay thin; backend logic belongs in `planning/`, `execution/`, and `backend/`.
