# Tech Debt Tracker

These are the current architecture and process questions that remain intentionally open.

## Open items

### Explain purity vs rich provider descriptions

- Current state: `explain()` may compile provider-owned fragments to produce provider plan descriptions.
- Question: should `explain()` stay on that model, or should providers get a describe-only path that avoids compile-time side effects?

### Non-`ROWS` window frames

- Current state: `RANGE` and `GROUPS` frame modes are explicitly rejected.
- Question: implement correct semantics later or continue treating them as out of scope?

### Mechanical plan-coverage enforcement

- Current state: substantial work is expected to have a checked-in execution plan, but enforcement is social/documented rather than diff-based.
- Question: add a low-noise repo check later, or keep it as documented workflow only?
