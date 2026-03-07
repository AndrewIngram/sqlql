# AGENTS.md

## Verification

For any non-trivial code change, always run:

- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`

Do this before finalizing the work unless the user explicitly asks not to.

## Reporting

If any verification step fails:

- fix the failure when it is in scope
- otherwise report the failure clearly in the final response

If a verification step cannot be run, state that explicitly and explain why.
