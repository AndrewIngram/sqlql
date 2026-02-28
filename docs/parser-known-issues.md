# Parser Known Issues (`node-sql-parser`)

This document tracks parser-specific behavior that currently adds complexity to `sqlql`.
If we later replace the parser, these are high-priority improvements.

## 1) Boolean precedence in AST (`AND` / `OR`)

Observed with current parser output:

- SQL: `a OR b AND c`
- AST: `(a OR b) AND c`
- SQL standard expectation: `a OR (b AND c)`

Impact:

- We cannot trust raw parser boolean tree shape for precedence-sensitive predicates.
- `sqlql` currently normalizes boolean expression trees before planning/evaluation.

Desired behavior in a future parser:

- Emit precedence-correct AST by default.
- Preserve explicit parenthesis grouping.

## 2) Dialect tradeoff for `RIGHT` / `FULL` joins

Observed:

- Default parser mode accepts `RIGHT JOIN` and `FULL JOIN`.
- `database: "sqlite"` mode rejects `RIGHT JOIN` / `FULL JOIN` syntax.

Impact:

- We cannot switch parser mode to SQLite just to improve parity behavior.
- We keep default parser mode and handle SQLite parity in tests at execution level.

Desired behavior in a future parser:

- Consistent parse support for target SQL features independent of dialect mode.
- Optional strict mode to reject unsupported features explicitly at validation stage.

## 3) Operator normalization burden for planning pushdown

Observed:

- Parser emits operators as raw strings (`=`, `IN`, `BETWEEN`, etc.).
- Not all operators map directly to current scan pushdown clauses.

Impact:

- Pushdown parsing must treat unsupported operators as "not pushdown-safe" (fallback), not hard errors.
- This adds defensive logic around filter extraction.

Desired behavior in a future parser:

- Stable, typed operator enums.
- Optional normalized predicate IR (including range predicates) to reduce planner glue code.

## 4) Function-style predicate encoding

Observed:

- Some predicates (e.g. `NOT`, `EXISTS`) arrive in function-style AST nodes.

Impact:

- Predicate evaluation must support both binary operator nodes and function-style predicates.

Desired behavior in a future parser:

- Unified predicate node kinds for unary/binary/logical operations.
- Reduced evaluator branching for equivalent semantics.

## Why keep this document

Parser replacement is not a short-term priority, but this list keeps design pressure visible:

- what is parser behavior vs executor behavior
- where we currently compensate in `sqlql`
- what would simplify if we owned or replaced the parser
