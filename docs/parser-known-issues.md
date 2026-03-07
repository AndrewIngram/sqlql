# Parser Notes (In-House SQLite Parser)

`sqlql` now uses an in-house SQL parser focused on the SQLite read-query subset that `sqlql`
supports.

This document tracks parser-specific behavior and known gaps so support decisions remain explicit.

## Baseline and Scope

- Baseline target: SQLite semantics as exercised by the parity suite (`better-sqlite3` runtime).
- Parser scope: read queries (`SELECT`, `WITH`, joins, set ops, predicates, aggregates, windows in supported subset).
- Non-goals in parser: write statements, parser fallback modes, and multi-statement execution.

## Known Parser Gaps

## 1) Coverage is intentionally subset-based, not full SQLite grammar

The parser is designed around `sqlql`'s supported surface area, not full SQLite syntax.
Queries outside the supported subset should fail fast with clear errors.

Current examples of intentionally unsupported syntax:

- recursive CTE execution
- correlated subqueries
- subqueries in `FROM`
- named `WINDOW` clauses/references
- explicit window frame clauses
- some advanced value/navigation window functions (for example `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`)

## 2) AST remains planner/executor-oriented

The internal parser emits AST nodes shaped to match the current planner/executor pipeline
(for example, function-style `NOT`/`EXISTS` nodes and specific window node structures).

Impact:

- parser and executor evolution are coupled
- AST schema changes must be made alongside planner/executor updates

## 3) Expression/operator support is bounded to current planning/evaluation needs

The parser recognizes the operators and expression forms used by current query planning and
evaluation logic. Unsupported operators are rejected or flow into "not pushdown-safe" paths.

Impact:

- adding new SQL operators typically requires both parser and planner/evaluator changes

## 4) Error diagnostics are intentionally simple

Parser errors prioritize stable behavior over rich diagnostics. Errors include message + token
position, but they are not full compiler-style diagnostics.

## Test Anchors

- `test/parser/sqlite-parser.test.ts`: parser-conformance coverage (shape + rejection behavior)
- `test/compliance/*-parity.test.ts`: SQLite parity behavior for supported query shapes
- `test/compliance/standards-gaps.todo.test.ts`: explicit unsupported feature backlog
