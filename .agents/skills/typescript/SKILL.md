---
name: typescript
description: Use when changing TypeScript types, refactoring signatures, or reviewing typing style. Prefer inference for internal functions and locals; keep explicit return types only when they carry API intent or are required for correctness, such as overloads or unstable generic or recursive inference.
---

# typescript

Apply these preferences when editing TypeScript in this repo:

- Omit explicit return types on internal functions unless they are necessary.
- Keep explicit return types when they define public API intent, support overloads, or prevent degraded inference in generic or recursive code.
- Treat broad result aliases as boundary contracts, not default internal implementation types.
- Prefer letting `better-result` infer concrete error unions for internal helpers instead of widening to a shared error alias.
- Use `satisfies` to check object shape without replacing inferred types.
- Add or update compile-time tests when changing public signatures or important inferred types.
- Treat casts as suspicious by default: if a cast exists only to force a helper or factory result into the desired shape, first ask whether the owning types should preserve that information directly.

When choosing between inference and annotation:

- Default to inference for local variables and non-exported helpers.
- Add an explicit annotation when it improves readability, pins a public contract, or avoids incorrect inference.
- Remove casts that only exist to force a broader type than the implementation actually produces.
- Remove casts that only compensate for lost generic information when the owning API can be fixed instead.
- Reintroduce an explicit annotation only if typechecking, declaration output, or call-site ergonomics become worse without it.
