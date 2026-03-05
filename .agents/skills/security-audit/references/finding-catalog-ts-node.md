# TS/Node Finding Catalog

Use this catalog to speed focused auditing.

## Query/Expression Injection

Look for:
- string interpolation in SQL/query fragments
- concatenated filter/order expressions
- ad-hoc parser bypasses

Useful search patterns:
- `rg -n "SELECT .*\\$\\{|\\+\s*sql|raw\(|unsafe|eval\(" src`
- `rg -n "orderBy|where|filter|sql" src`

False-positive filter:
- parameterized APIs with strict binding and no expression interpolation

## Schema Facade Bypass / Data Overexposure

Look for:
- schema exposure checks applied too late
- planner nodes referencing columns not validated against facade
- wildcard projection leakage through adapter-specific paths

Useful search patterns:
- `rg -n "validate.*schema|column|projection|select|expose" src`
- `rg -n "scan|project|join|orderBy|where" src`

False-positive filter:
- explicit allowlist checks at planning/lowering stage before execution

## Auth Boundary Omission

Look for:
- context-derived tenant/user constraints created but not attached
- optional auth filters dropped in fallback paths
- provider hooks that ignore caller scope

Useful search patterns:
- `rg -n "context|tenant|org|scope|auth|policy" src`
- `rg -n "fallback|local" src`

False-positive filter:
- provable mandatory scope enforcement in all execution branches

## Unsafe Dynamic Evaluation

Look for:
- `eval`, `Function`, vm execution, dynamic import with untrusted strings
- permissive JSON parsing feeding executable paths

Useful search patterns:
- `rg -n "eval\(|new Function|vm\.|import\(" src`

False-positive filter:
- static code execution with trusted literals only

## Availability Abuse / DoS

Look for:
- unbounded scans, joins, or recursive branches
- max-row/time checks enforced only after heavy work
- fallback paths that materialize large intermediates

Useful search patterns:
- `rg -n "limit|max|timeout|guardrail|batch" src`
- `rg -n "fallback|materialize|parallel|join|aggregate" src`

False-positive filter:
- strict pre-execution limits and predictable complexity bounds
